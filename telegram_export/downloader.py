#!/bin/env python3
import asyncio
import datetime
import itertools
import os
import time
from collections import defaultdict

import arrow
from telethon import utils
from telethon.errors import ChatAdminRequiredError, ChatWriteForbiddenError
from telethon.tl import types, functions

from telegram_export.database import db_media, db_user, db_super_group, db_channel, db_message
from telegram_export.logger import create_logger
from telegram_export.progress import EntityProgress, MediaProgress, MessageProgress
from telegram_export.utils import fix_windows_filename
from . import utils as export_utils

logger = create_logger('downloader')

VALID_TYPES = {
    'photo', 'document', 'video', 'audio', 'sticker', 'voice', 'chatphoto'
}
QUEUE_TIMEOUT = 5
DOWNLOAD_PART_SIZE = 256 * 1024

# How long should we sleep between these requests? These numbers
# should be tuned to adjust (n requests/time spent + flood time).
USER_FULL_DELAY = 0  # 1.5
CHAT_FULL_DELAY = 0  # 1.5
MEDIA_DELAY = 0  # 3.0
HISTORY_DELAY = 0  # 1.0


class Downloader:
    """
    Download dialogs and their associated data, and dump them.
    Make Telegram API requests and sleep for the appropriate time.
    """

    def __init__(self, client, config, dumper, loop=None):
        self.client = client
        self.loop = loop or asyncio.get_event_loop()
        self.max_size = config.getint('MaxSize')
        self.min_size = config.getint('MinSize')
        self.types = {x.strip().lower()
                      for x in (config.get('MediaWhitelist') or '').split(',')
                      if x.strip()}
        self.media_fmt = os.path.join(config['OutputDirectory'], config['MediaFilenameFmt'])
        assert all(x in VALID_TYPES for x in self.types)
        if self.types:
            self.types.add('unknown')  # Always allow "unknown" media types

        self.dumper = dumper
        self._checked_entity_ids = set()
        self._media_bar = None

        # To get around the fact we always rely on the database to download
        # media (which simplifies certain operations and ensures that the
        # resulting filename are always the same) but this (the db) might not
        # have some entities dumped yet, we save the only needed information
        # in memory for every dump, that is, {peer_id: display}.
        self._displays = {}

        # This field keeps track of the download in progress if any, so that
        # partially downloaded files can be deleted. Only one file can be
        # downloaded at any given time, so using a set here makes no sense.
        self._incomplete_download = None

        # We're gonna need a few queues if we want to do things concurrently.
        # None values should be inserted to notify that the dump has finished.
        self._media_queue = asyncio.Queue()
        self._user_queue = asyncio.Queue()
        self._chat_queue = asyncio.Queue()
        self._running = False

        self.running_message = MessageProgress()
        self.running_entity = EntityProgress()
        self.running_media_list = {}

    def _check_media(self, media):
        """检查是否应下载相应的文件"""
        if not media or not self.max_size:
            return False
        if not self.types:
            return True
        if not getattr(media, 'document', None):
            return False
        return export_utils.get_media_type(media) in self.types

    def _dump_full_entity(self, entity):
        """
        Dumps the full entity into the Dumper, also enqueuing their profile
        photo if any so it can be downloaded later by a different coroutine.
        Supply None as the photo_id if self.types is empty or 'chatphoto' is
        not in self.types
        """
        if isinstance(entity, types.UserFull):
            if not self.types or 'chatphoto' in self.types:
                photo_id = self.dumper.dump_media(entity.profile_photo)
            else:
                photo_id = None
            self.enqueue_photo(entity.profile_photo, photo_id, entity.user)
            self.dumper.dump_user(entity, photo_id=photo_id)

        elif isinstance(entity, types.Chat):
            if not self.types or 'chatphoto' in self.types:
                photo_id = self.dumper.dump_media(entity.photo)
            else:
                photo_id = None
            self.enqueue_photo(entity.photo, photo_id, entity)
            self.dumper.dump_chat(entity, photo_id=photo_id)

        elif isinstance(entity, types.messages.ChatFull):
            if not self.types or 'chatphoto' in self.types:
                photo_id = self.dumper.dump_media(entity.full_chat.chat_photo)
            else:
                photo_id = None
            chat = next(
                x for x in entity.chats if x.id == entity.full_chat.id
            )
            self.enqueue_photo(entity.full_chat.chat_photo, photo_id, chat)
            if chat.megagroup:
                self.dumper.dump_supergroup(entity.full_chat, chat,
                                            photo_id)
            else:
                self.dumper.dump_channel(entity.full_chat, chat, photo_id)

    def _dump_messages(self, messages, target):
        """
        Helper method to iterate the messages from a GetMessageHistoryRequest
        and dump them into the Dumper, mostly to avoid excessive nesting.

        Also enqueues any media to be downloaded later by a different coroutine.
        """
        for m in messages:
            if isinstance(m, types.Message):
                media_id = self.dumper.dump_media(m.media)
                if media_id and self._check_media(m.media):
                    self.enqueue_media(
                        media_id, utils.get_peer_id(target), m.from_id, m.date
                    )

                self.dumper.dump_message(
                    message=m,
                    context_id=utils.get_peer_id(target),
                    forward_id=self.dumper.dump_forward(m.fwd_from),
                    media_id=media_id
                )
            elif isinstance(m, types.MessageService):
                if isinstance(m.action, types.MessageActionChatEditPhoto):
                    media_id = self.dumper.dump_media(m.action.photo)
                    self.enqueue_photo(m.action.photo, media_id, target,
                                       peer_id=m.from_id, date=m.date)
                else:
                    media_id = None
                self.dumper.dump_message_service(
                    message=m,
                    context_id=utils.get_peer_id(target),
                    media_id=media_id
                )

    def _dump_admin_log(self, events, target):
        """
        Helper method to iterate the events from a GetAdminLogRequest
        and dump them into the Dumper, mostly to avoid excessive nesting.

        Also enqueues any media to be downloaded later by a different coroutine.
        """
        for event in events:
            assert isinstance(event, types.ChannelAdminLogEvent)
            if isinstance(event.action,
                          types.ChannelAdminLogEventActionChangePhoto):
                media_id1 = self.dumper.dump_media(event.action.new_photo)
                media_id2 = self.dumper.dump_media(event.action.prev_photo)
                self.enqueue_photo(event.action.new_photo, media_id1, target,
                                   peer_id=event.user_id, date=event.date)
                self.enqueue_photo(event.action.prev_photo, media_id2, target,
                                   peer_id=event.user_id, date=event.date)
            else:
                media_id1 = None
                media_id2 = None
            self.dumper.dump_admin_log_event(
                event, utils.get_peer_id(target), media_id1, media_id2
            )
        return min(e.id for e in events)

    def _get_name(self, peer_id):
        if peer_id is None:
            return ''

        name = self._displays.get(peer_id)
        if name:
            return name

        # c = self.dumper.conn.cursor()
        _, kind = utils.resolve_id(peer_id)
        if kind == types.PeerUser:
            row = db_user.find_one({'id': peer_id})
            if row:
                return f'{row["first_name"] or ""} {row["last_name"] or ""}'.strip()
        elif kind == types.PeerChat:
            row = db_user.find_one({'id': peer_id})
            if row:
                return row['title']
        elif kind == types.PeerChannel:
            row = db_channel.find_one({'id': peer_id})
            if row:
                return row['title']
            row = db_super_group.find_one({'id': peer_id})
            if row:
                return row['title']
        return ''

    async def _download_media(self, media_id, context_id, sender_id, date, progress):
        media_row = db_media.find_one({'_id': media_id})
        progress.name = media_row['name']
        if media_row['size']:
            if media_row['size'] > self.max_size:
                logger.warning('忽略过大文件：%s', media_row['name'])
                return  # 忽略过大的文件
            if media_row['size'] < self.min_size:
                logger.warning('忽略过小文件：%s', media_row['name'])
                return  # 忽略过小的文件
        # Documents have attributes and they're saved under the "document"
        # namespace so we need to split it before actually comparing.
        media_type = media_row['type'].split('.')
        media_type, media_subtype = media_type[0], media_type[-1]
        if media_type not in ('document',):
            logger.info('忽略文档类型：%s', media_type)
            return  # Only photos or documents are actually downloadable

        formatter = defaultdict(
            str,
            context_id=context_id,
            sender_id=sender_id,
            type=media_subtype or 'unknown',
            name=self._get_name(context_id) or 'unknown',
            sender_name=self._get_name(sender_id) or 'unknown'
        )

        # Documents might have a filename, which may have an extension. Use
        # the extension from the filename if any (more accurate than mime).
        ext = None
        filename = media_row['name']
        if filename:
            filename, ext = os.path.splitext(filename)
        else:
            # No filename at all, set a sensible default filename
            filename = arrow.get(date).format('YYYYMMDDHHmmssSSS')
            logger.debug('忽略无名称文件')
            return

        # The saved media didn't have a filename and we set our own.
        # Detect a sensible extension from the known mimetype.
        if not ext:
            ext = export_utils.get_extension(media_row['mime_type'])

        # Apply the date to the user format string and then replace the map
        formatter['filename'] = fix_windows_filename(filename)
        filename = date.strftime(self.media_fmt).format_map(formatter)
        filename += '.{}{}'.format(media_id, ext)
        if os.path.isfile(filename):
            logger.debug('Skipping already-existing file %s', filename)
            return
        logger.info('正在下载：%s 至 %s', media_type, filename)
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        if media_type == 'document':
            location = types.InputDocumentFileLocation(
                id=media_row['local_id'],
                version=media_row['volume_id'],
                access_hash=media_row['secret']
            )
        else:
            location = types.InputFileLocation(
                local_id=media_row['local_id'],
                volume_id=media_row['volume_id'],
                secret=media_row['secret']
            )

        def progress_callback(saved, total):
            """Increment the tqdm progress bar"""
            if total is None:
                # No size was found so the bar total wasn't incremented before
                progress.total += saved
                progress.inc(saved)
            elif saved == total:
                # Downloaded the last bit (which is probably <> part size)
                mod = (saved % DOWNLOAD_PART_SIZE) or DOWNLOAD_PART_SIZE
                progress.inc(mod)
            else:
                # All chunks are of the same size and this isn't the last one
                progress.inc(DOWNLOAD_PART_SIZE)

        if media_row['size'] is not None:
            progress.total += media_row['size']

        self._incomplete_download = filename
        await self.client.download_file(
            location, file=filename, file_size=media_row['size'],
            part_size_kb=DOWNLOAD_PART_SIZE // 1024,
            progress_callback=progress_callback
        )
        self._incomplete_download = None

    async def _media_consumer(self, queue, name, index):
        while self._running:
            self.running_media_list[index] = MediaProgress(name)
            start = time.time()
            media_id, context_id, sender_id, date = await queue.get()
            await self._download_media(media_id, context_id, sender_id,
                                       datetime.datetime.utcfromtimestamp(date),
                                       self.running_media_list[index])
            queue.task_done()
            await asyncio.sleep(max(MEDIA_DELAY - (time.time() - start), 0), loop=self.loop)

    async def _user_consumer(self, queue):
        while self._running:
            start = time.time()
            self._dump_full_entity(await self.client(functions.users.GetFullUserRequest(await queue.get())))
            queue.task_done()
            self.running_entity.inc()
            await asyncio.sleep(max(USER_FULL_DELAY - (time.time() - start), 0), loop=self.loop)

    async def _chat_consumer(self, queue):
        while self._running:
            start = time.time()
            chat = await queue.get()
            if isinstance(chat, (types.Chat, types.PeerChat)):
                self._dump_full_entity(chat)
            else:  # isinstance(chat, (types.Channel, types.PeerChannel)):
                self._dump_full_entity(await self.client(
                    functions.channels.GetFullChannelRequest(chat)
                ))
            queue.task_done()
            self.running_entity.inc()
            await asyncio.sleep(max(CHAT_FULL_DELAY - (time.time() - start), 0), loop=self.loop)

    def enqueue_entities(self, entities):
        """
        Enqueues the given iterable of entities to be dumped later by a
        different coroutine. These in turn might enqueue profile photos.
        """
        for entity in entities:
            eid = utils.get_peer_id(entity)
            self._displays[eid] = utils.get_display_name(entity)
            if isinstance(entity, types.User):
                if entity.deleted or entity.min:
                    continue  # Empty name would cause IntegrityError
            elif isinstance(entity, types.Channel):
                if entity.left:
                    continue  # Getting full info triggers ChannelPrivateError
            elif not isinstance(entity, (types.Chat,
                                         types.InputPeerUser,
                                         types.InputPeerChat,
                                         types.InputPeerChannel)):
                # Drop UserEmpty, ChatEmpty, ChatForbidden and ChannelForbidden
                continue

            if eid in self._checked_entity_ids:
                continue
            else:
                self._checked_entity_ids.add(eid)
                if isinstance(entity, (types.User, types.InputPeerUser)):
                    self._user_queue.put_nowait(entity)
                else:
                    self._chat_queue.put_nowait(entity)

    def enqueue_media(self, media_id, context_id, sender_id, date):
        """
        Enqueues the given message or media from the given context entity
        to be downloaded later. If the ID of the message is known it should
        be set in known_id. The media won't be enqueued unless its download
        is desired.
        """
        if not date:
            date = int(time.time())
        elif not isinstance(date, int):
            date = int(date.timestamp())
        self._media_queue.put_nowait((media_id, context_id, sender_id, date))

    def enqueue_photo(self, photo, photo_id, context,
                      peer_id=None, date=None):
        if not photo_id:
            return
        if not isinstance(context, int):
            context = utils.get_peer_id(context)
        if peer_id is None:
            peer_id = context
        if date is None:
            date = getattr(photo, 'date', None) or datetime.datetime.now()
        self.enqueue_media(photo_id, context, peer_id, date)

    async def start(self, entity):
        """
        Starts the dump with the given target ID.
        """
        self._running = True
        self._incomplete_download = None
        target_in = await self.client.get_input_entity(entity)
        target = await self.client.get_entity(target_in)
        entity = utils.get_peer_id(target)

        chat_name = utils.get_display_name(target)
        logger.info(f'开始爬取【{chat_name}】')

        found = self.dumper.get_message_count(entity)
        logger.info(f'已转储【{found}】条消息')
        self.running_message = MessageProgress(chat_name)
        self.running_entity = EntityProgress(chat_name)

        user_consumers = [asyncio.ensure_future(self._user_consumer(self._user_queue), loop=self.loop)
                          for _ in range(1)]
        chat_consumers = [asyncio.ensure_future(self._chat_consumer(self._chat_queue), loop=self.loop)
                          for _ in range(1)]
        media_consumers = [asyncio.ensure_future(self._media_consumer(self._media_queue, chat_name, i), loop=self.loop)
                           for i in range(3)]

        self.enqueue_entities(self.dumper.iter_resume_entities(entity))
        for mid, sender_id, date in self.dumper.iter_resume_media(entity):
            self.enqueue_media(mid, entity, sender_id, date)

        try:
            self.enqueue_entities((target,))
            self.running_entity.total = len(self._checked_entity_ids)
            req = functions.messages.GetHistoryRequest(
                peer=target_in,
                offset_id=0,
                offset_date=None,
                add_offset=0,
                limit=self.dumper.chunk_size,
                max_id=0,
                min_id=0,
                hash=0
            )
            if isinstance(target_in, types.InputPeerChat) or (
                    isinstance(target, types.Channel) and (target.megagroup or target.admin_rights is not None)):
                try:
                    logger.info('获取群组参与人员列表')
                    participants = await self.client.get_participants(target_in)
                    added, removed = self.dumper.dump_participants_delta(entity, ids=[x.id for x in participants])
                    logger.info(f'群组【{chat_name}】新加入成员{len(added)}名，退出成员{len(removed)}', )
                except ChatAdminRequiredError:
                    logger.error('获取群组成员失败，权限不足')
            ret = self.dumper.get_resume(entity)
            req.offset_id, req.offset_date, stop_at = ret['id'], ret['date'], ret['stop_at']
            if req.offset_id:
                logger.info('Resuming at %s (%s)', req.offset_date, req.offset_id)

            # Check if we have access to the admin log
            # TODO Resume admin log?
            # Rather silly considering logs only last up to two days and
            # there isn't much information in them (due to their short life).
            if isinstance(target_in, types.InputPeerChannel):
                logger.info('开始爬取AdminLog')
                log_req = functions.channels.GetAdminLogRequest(
                    target_in, q='', min_id=0, max_id=0, limit=1
                )
                try:
                    await self.client(log_req)
                    log_req.limit = 100
                except ChatAdminRequiredError:
                    log_req = None
                except ChatWriteForbiddenError:
                    log_req = None
            else:
                log_req = None

            chunks_left = self.dumper.max_chunks
            # This loop is for get history, although the admin log
            # is interlaced as well to dump both at the same time.
            while self._running:
                start = time.time()
                history = await self.client(req)
                # Queue found entities so they can be dumped later
                self.enqueue_entities(itertools.chain(
                    history.users, history.chats
                ))
                self.running_entity.total = len(self._checked_entity_ids)

                # Dump the messages from this batch
                self._dump_messages(history.messages, target)

                # Determine whether to continue dumping or we're done
                count = len(history.messages)
                self.running_message.total = getattr(history, 'count', count)
                self.running_message.inc(count)
                if history.messages:
                    # We may reinsert some we already have (so found > total)
                    found = min(found + len(history.messages), self.running_message.total)
                    req.offset_id = min(m.id for m in history.messages)
                    req.offset_date = min(m.date for m in history.messages)

                # Receiving less messages than the limit means we have
                # reached the end, so we need to exit. Next time we'll
                # start from offset 0 again so we can check for new messages.
                #
                # We dump forward (message ID going towards 0), so as soon
                # as the minimum message ID (now in offset ID) is less than
                # the highest ID ("closest" bound we need to reach), stop.
                if count < req.limit or req.offset_id <= stop_at:
                    logger.debug('Received less messages than limit, done.')
                    max_id = self.dumper.get_max_message_id(entity) or 0  # can't have NULL
                    self.dumper.save_resume(entity, stop_at=max_id)
                    break

                # Keep track of the last target ID (smallest one),
                # so we can resume from here in case of interruption.
                self.dumper.save_resume(
                    entity, msg=req.offset_id, msg_date=req.offset_date,
                    stop_at=stop_at  # We DO want to preserve stop_at.
                )

                chunks_left -= 1  # 0 means infinite, will reach -1 and never 0
                if chunks_left == 0:
                    logger.debug('Reached maximum amount of chunks, done.')
                    break

                # Interlace with the admin log request if any
                if log_req:
                    result = await self.client(log_req)
                    self.enqueue_entities(itertools.chain(
                        result.users, result.chats
                    ))
                    if result.events:
                        log_req.max_id = self._dump_admin_log(result.events, target)
                    else:
                        log_req = None

                # We need to sleep for HISTORY_DELAY but we have already spent
                # some of it invoking (so subtract said delta from the delay).
                await asyncio.sleep(max(HISTORY_DELAY - (time.time() - start), 0), loop=self.loop)

            # Message loop complete, wait for the queues to empty
            # self.running_message.n = self.running_message.total
            # self.running_message.close()
            # self.dumper.commit()

            # This loop is specific to the admin log (to finish up)
            while log_req and self._running:
                start = time.time()
                result = await self.client(log_req)
                self.enqueue_entities(itertools.chain(
                    result.users, result.chats
                ))
                if result.events:
                    log_req.max_id = self._dump_admin_log(result.events, target)
                    await asyncio.sleep(max(HISTORY_DELAY - (time.time() - start), 0), loop=self.loop)
                else:
                    log_req = None

            logger.info('Done. Retrieving full information about %s missing entities.',
                        self._user_queue.qsize() + self._chat_queue.qsize())
            await self._user_queue.join()
            await self._chat_queue.join()
            await self._media_queue.join()
        finally:
            self._running = False
            # If the download was interrupted and there are users left in the
            # queue we want to save them into the database for the next run.
            entities = []
            while not self._user_queue.empty():
                entities.append(self._user_queue.get_nowait())
            while not self._chat_queue.empty():
                entities.append(self._chat_queue.get_nowait())
            if entities:
                self.dumper.save_resume_entities(entity, entities)

            # Do the same with the media queue
            media = []
            while not self._media_queue.empty():
                media.append(self._media_queue.get_nowait())
            self.dumper.save_resume_media(media)

            # 删除下载了一部分的文件
            if self._incomplete_download is not None and os.path.isfile(self._incomplete_download):
                os.remove(self._incomplete_download)
        logger.info('Download Exit')

    async def download_past_media(self, dumper, target_id):
        """
        下载已经转储到数据库但尚未下载的媒体。
        格式化文件名之后已存在的文件的媒体将被忽略，不会再次重新下载。
        """
        # TODO Should this respect and download only allowed media? Or all?
        target_in = await self.client.get_input_entity(target_id)
        target = await self.client.get_entity(target_in)
        target_id = utils.get_peer_id(target)

        rows = db_message.find({'context_id': target_id, "check": {'$ne': None}})
        for row in rows:
            await self._media_queue.put((
                row['media_id'],
                target_id,
                row['from_id'],
                datetime.datetime.utcfromtimestamp(row['date'])
            ))
        chat_name = utils.get_display_name(target)
        media_consumers = [asyncio.ensure_future(self._media_consumer(self._media_queue, chat_name, i), loop=self.loop)
                           for i in range(10)]
        await asyncio.wait(media_consumers)
