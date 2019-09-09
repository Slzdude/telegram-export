"""A module for dumping export data into the database"""
import logging
import time
from base64 import b64encode
from datetime import datetime
from enum import Enum

import arrow
from pymongo import UpdateOne
from telethon.tl import types
from telethon.utils import get_peer_id, resolve_id, get_input_peer

from telegram_export import utils
from telegram_export.database import db_self_info, db_admin_log, db_message, db_user, db_channel, db_super_group, \
    db_chat_participants, db_media, db_forward, db_resume, db_resume_media, db_resume_entity, \
    update_by_invalidation_time, db_chat, update_by_id, db_dialog

logger = logging.getLogger(__name__)

DB_VERSION = 1  # database version


class InputFileType(Enum):
    """An enum to specify the type of an InputFile"""
    NORMAL = 0
    DOCUMENT = 1


def sanitize_dict(dictionary):
    """
    Sanitizes a dictionary, encoding all bytes as
    Base64 so that it can be serialized as JSON.

    Assumes that there are no containers with bytes inside,
    and that the dictionary doesn't contain self-references.
    """
    for k, v in dictionary.items():
        if isinstance(v, bytes):
            dictionary[k] = str(b64encode(v), encoding='ascii')
        elif isinstance(v, datetime):
            dictionary[k] = v.timestamp()
        elif isinstance(v, dict):
            sanitize_dict(v)
        elif isinstance(v, list):
            for d in v:
                if isinstance(d, dict):
                    sanitize_dict(d)


class Dumper:
    """Class to interface with the database for exports"""

    def __init__(self, config):
        """
        Initialise the dumper. `config` should be a dict-like
        object from the config file's Dumper section".
        """
        self.config = config
        self.chunk_size = max(int(config.get('ChunkSize', 100)), 1)
        self.max_chunks = max(int(config.get('MaxChunks', 0)), 0)
        self.invalidation_time = max(config.getint('InvalidationTime', 0), -1)

        self.dump_methods = ['message', 'user', 'message_service', 'channel',
                             'supergroup', 'chat', 'adminlog_event', 'media',
                             'participants_delta', 'media', 'forward']

        self._dump_callbacks = {method: set() for method in self.dump_methods}

    # TODO make these callback functions less repetitive.
    # For the most friendly API, we should  have different methods for each
    # kind of callback, but there could be a way to make this cleaner.
    # Perhaps a dictionary mapping 'message' to the message callback set.

    def add_callback(self, dump_method, callback):
        """
        Add the callback function to the set of callbacks for the given
        dump method. dump_method should be a string, and callback should be a
        function which takes one argument - a tuple which will be dumped into
        the database. The list of valid dump methods is await dumper.dump_methods.
        If the dumper does not dump a row due to the invalidation_time, the
        callback will still be called.
        """
        if dump_method not in self.dump_methods:
            raise ValueError(
                f"Cannot attach callback to method {dump_method}. Available "
                f"methods are {self.dump_methods}")

        self._dump_callbacks[dump_method].add(callback)

    def remove_callback(self, dump_method, callback):
        """
        Remove the callback function from the set of callbacks for the given
        dump method. Will raise KeyError if the callback is not in the set of
        callbacks for that method
        """
        if dump_method not in self.dump_methods:
            raise ValueError(
                f"Cannot remove callback from method {dump_method}. Available "
                f"methods are {self.dump_methods}")

        self._dump_callbacks[dump_method].remove(callback)

    async def check_self_user(self, self_id):
        """
        Checks the self ID. If there is a stored ID and it doesn't match the
        given one, an error message is printed and the application exits.
        """
        ret = await db_self_info.find_one()
        if ret and ret.get('user_id') != self_id:
            logger.error('数据库归属于其他用户！')
            exit(1)
        await db_self_info.update_one({'user_id': self_id}, {'$set': {'user_id': self_id}}, upsert=True)
        logger.info('数据用户身份校验通过！')

    async def dump_dialog(self, dialog):
        row = {
            'id': dialog.id,
            'name': dialog.name,
            'title': dialog.title,
            'is_channel': dialog.is_channel,
            'is_group': dialog.is_group,
            'is_user': dialog.is_user,
            'date': dialog.date.timestamp(),
        }  # No MessageAction

        for callback in self._dump_callbacks['message']:
            callback(row)
        return await update_by_id(db_dialog, row)

    async def dump_message(self, message, context_id, forward_id, media_id):
        """
        Dump a Message into the Message table.

        Params:
            Message to dump,
            ID of the chat dumping,
            ID of Forward in the DB (or None),
            ID of message Media in the DB (or None)

        Returns:
            Inserted row ID.
        """
        if not message.message and message.media:
            message.message = getattr(message.media, 'caption', '')

        row = {
            'id': message.id,
            'context_id': context_id,
            'date': message.date.timestamp(),
            'from_id': message.from_id,
            'message': message.message,
            'reply_message_id': message.reply_to_msg_id,
            'forward_id': forward_id,
            'post_author': message.post_author,
            'view_count': message.views,
            'media_id': media_id,
            'formatting': utils.encode_msg_entities(message.entities),
            'service_action': None
        }  # No MessageAction

        for callback in self._dump_callbacks['message']:
            callback(row)
        return await update_by_id(db_message, row)

    async def dump_message_service(self, message, context_id, media_id):
        """Similar to self.dump_message, but for MessageAction's."""
        name = utils.action_to_name(message.action)
        if not name:
            return

        extra = message.action.to_dict()
        del extra['_']  # We don't need to store the type, already have name
        sanitize_dict(extra)
        # extra = json.dumps(extra)

        row = {
            'id': message.id,
            'context_id': context_id,
            'date': message.date.timestamp(),
            'from_id': message.from_id,
            'message': extra,  # Message field contains the information
            'reply_message_id': message.reply_to_msg_id,
            'forward_id': None,  # No forward
            'post_author': None,  # No author
            'view_count': None,  # No views
            'media_id': media_id,  # Might have e.g. a new chat Photo
            'formatting': None,  # No entities
            'service_action': name
        }

        for callback in self._dump_callbacks['message_service']:
            callback(row)

        return await update_by_id(db_message, row)

    async def dump_admin_log_event(self, event, context_id, media_id1, media_id2):
        """Similar to self.dump_message_service but for channel actions."""
        name = utils.action_to_name(event.action)
        if not name:
            return

        extra = event.action.to_dict()
        del extra['_']  # We don't need to store the type, already have name
        sanitize_dict(extra)
        # extra = json.dumps(extra)

        row = {
            'id': event.id,
            'context_id': context_id,
            'date': event.date.timestamp(),
            'user_id': event.user_id,
            'media_id_1': media_id1,
            'media_id_2': media_id2,
            'action': name,
            'data': extra
        }

        for callback in self._dump_callbacks['adminlog_event']:
            callback(row)

        return await update_by_id(db_admin_log, row)

    async def dump_user(self, user_full, photo_id, timestamp=None):
        """Dump a UserFull into the User table
        Params: UserFull to dump, MediaID of the profile photo in the DB
        Returns -, or False if not added"""
        # Rationale for UserFull rather than User is to get bio
        row = {
            'id': user_full.user.id,
            'date_updated': timestamp or round(time.time()),
            'first_name': user_full.user.first_name,
            'last_name': user_full.user.last_name,
            'username': user_full.user.username,
            'phone': user_full.user.phone,
            'about': user_full.about,
            'bot': user_full.user.bot,
            'common_chats_count': user_full.common_chats_count,
            'photo_id': photo_id
        }

        for callback in self._dump_callbacks['user']:
            callback(row)

        return await update_by_invalidation_time(db_user, row, self.invalidation_time)

    async def dump_channel(self, channel_full, channel, photo_id, timestamp=None):
        """Dump a Channel into the Channel table.
        Params: ChannelFull, Channel to dump, MediaID
                of the profile photo in the DB
        Returns -"""
        # Need to get the full object too for 'about' info
        row = {
            'id': get_peer_id(channel),
            'date_updated': timestamp or round(time.time()),
            'about': channel_full.about,
            'title': channel.title,
            'username': channel.username,
            'photo_id': photo_id,
            'pin_message_id': channel_full.pinned_msg_id
        }

        for callback in self._dump_callbacks['channel']:
            callback(row)

        return await update_by_invalidation_time(db_channel, row, self.invalidation_time)

    async def dump_supergroup(self, supergroup_full, supergroup, photo_id,
                              timestamp=None):
        """Dump a Supergroup into the Supergroup table
        Params: ChannelFull, Channel to dump, MediaID
                of the profile photo in the DB.
        Returns -"""
        # Need to get the full object too for 'about' info
        row = {
            'id': get_peer_id(supergroup),
            'date_updated': timestamp or round(time.time()),
            'about': getattr(supergroup_full, 'about', None) or '',
            'title': supergroup.title,
            'username': supergroup.username,
            'photo_id': photo_id,
            'pin_message_id': supergroup_full.pinned_msg_id
        }

        for callback in self._dump_callbacks['supergroup']:
            callback(row)

        return await update_by_invalidation_time(db_super_group, row, self.invalidation_time)

    async def dump_chat(self, chat, photo_id, timestamp=None):
        """Dump a Chat into the Chat table
        Params: Chat to dump, MediaID of the profile photo in the DB
        Returns -"""
        if isinstance(chat.migrated_to, types.InputChannel):
            migrated_to_id = chat.migrated_to.channel_id
        else:
            migrated_to_id = None

        row = {
            'id': get_peer_id(chat),
            'date_updated': timestamp or round(time.time()),
            'title': chat.title,
            'migrated_to_id': migrated_to_id,
            'photo_id': photo_id
        }

        for callback in self._dump_callbacks['chat']:
            callback(row)

        return await update_by_invalidation_time(db_chat, row, self.invalidation_time)

    async def dump_participants_delta(self, context_id, ids):
        """
        Dumps the delta between the last dump of IDs for the given context ID
        and the current input user IDs.
        """
        ids = set(ids)
        cursor = db_chat_participants.find({'context_id': context_id})
        cursor.sort('date_updated', 1)
        count = await db_chat_participants.count_documents({'context_id': context_id})
        if not count:
            added = ids
            removed = set()
        else:
            ret = await cursor.to_list(count)
            # Build the last known list of participants from the saved deltas
            last_ids = set(ret[0]['added'])
            for row in ret[1:]:
                last_ids = set(row['added'])
                added = set(row['added'])
                removed = set(row['removed'])
                last_ids = (last_ids | added) - removed
            added = ids - last_ids
            removed = last_ids - ids

        row = {
            'context_id': context_id,
            'date_updated': round(time.time()),
            'added': list(added),
            'removed': list(removed)
        }
        for callback in self._dump_callbacks['participants_delta']:
            callback(row)

        await db_chat_participants.insert_one(row)
        return added, removed

    async def dump_media(self, media, media_type=None):
        """Dump a MessageMedia into the Media table
        Params: media Telethon object
        Returns: ID of inserted row"""
        if not media:
            return
        row = {
            'name': None,
            'mime_type': None,
            'size': None,
            'thumbnail_id': None,
            'local_id': None,
            'volume_id': None,
            'secret': None,
            'type': media_type,
            'extra': media.to_dict()
        }
        sanitize_dict(row['extra'])
        # row['extra'] = json.dumps(row['extra'])

        if isinstance(media, types.MessageMediaContact):
            row['type'] = 'contact'
            row['name'] = f'{media.first_name} {media.last_name}'
            row['local_id'] = media.user_id
            try:
                row['secret'] = int(media.phone_number or '0')
            except ValueError:
                row['secret'] = 0

        elif isinstance(media, types.MessageMediaDocument):
            row['type'] = utils.get_media_type(media)
            doc = media.document
            if isinstance(doc, types.Document):
                row['mime_type'] = doc.mime_type
                row['size'] = doc.size
                row['thumbnail_id'] = await self.dump_media(doc.thumb)
                row['local_id'] = doc.id
                row['volume_id'] = doc.version
                row['secret'] = doc.access_hash
                for attr in doc.attributes:
                    if isinstance(attr, types.DocumentAttributeFilename):
                        row['name'] = attr.file_name

        elif isinstance(media, types.MessageMediaEmpty):
            row['type'] = 'empty'
            return

        elif isinstance(media, types.MessageMediaGame):
            row['type'] = 'game'
            game = media.game
            if isinstance(game, types.Game):
                row['name'] = game.short_name
                row['thumbnail_id'] = await self.dump_media(game.photo)
                row['local_id'] = game.id
                row['secret'] = game.access_hash

        elif isinstance(media, types.MessageMediaGeo):
            row['type'] = 'geo'
            geo = media.geo
            if isinstance(geo, types.GeoPoint):
                row['name'] = f'({repr(geo.lat)}, {repr(geo.long)})'

        elif isinstance(media, types.MessageMediaGeoLive):
            row['type'] = 'geolive'
            geo = media.geo
            if isinstance(geo, types.GeoPoint):
                row['name'] = f'({repr(geo.lat)}, {repr(geo.long)})'

        elif isinstance(media, types.MessageMediaInvoice):
            row['type'] = 'invoice'
            row['name'] = media.title
            row['thumbnail_id'] = await self.dump_media(media.photo)

        elif isinstance(media, types.MessageMediaPhoto):
            row['type'] = 'photo'
            row['mime_type'] = 'image/jpeg'
            media = media.photo

        elif isinstance(media, types.MessageMediaUnsupported):
            row['type'] = 'unsupported'
            return

        elif isinstance(media, types.MessageMediaVenue):
            row['type'] = 'venue'
            row['name'] = f'{media.title} - {media.address} ({media.provider}, {media.venue_id} {media.venue_type})'
            geo = media.geo
            if isinstance(geo, types.GeoPoint):
                row['name'] += f' at ({repr(geo.lat)}, {repr(geo.long)})'

        elif isinstance(media, types.MessageMediaWebPage):
            row['type'] = 'webpage'
            web = media.webpage
            if isinstance(web, types.WebPage):
                row['name'] = web.title
                row['thumbnail_id'] = await self.dump_media(web.photo, 'thumbnail')
                row['local_id'] = web.id
                row['secret'] = web.hash

        if isinstance(media, types.Photo):
            # Extra fallback cases for common parts
            row['type'] = 'photo'
            row['mime_type'] = 'image/jpeg'
            row['name'] = arrow.get(media.date).format('YYYYMMDDHHmmssSSS') + '.jpg'
            sizes = [x for x in media.sizes
                     if isinstance(x, (types.PhotoSize, types.PhotoCachedSize))]
            if sizes:
                small = min(sizes, key=lambda s: s.w * s.h)
                large = max(sizes, key=lambda s: s.w * s.h)
                media = large
                if small != large:
                    row['thumbnail_id'] = await self.dump_media(small, 'thumbnail')

        if isinstance(media, (types.PhotoSize,
                              types.PhotoCachedSize,
                              types.PhotoSizeEmpty)):
            row['type'] = 'photo'
            row['mime_type'] = 'image/jpeg'
            if isinstance(media, types.PhotoSizeEmpty):
                row['size'] = 0
            else:
                if isinstance(media, types.PhotoSize):
                    row['size'] = media.size
                elif isinstance(media, types.PhotoCachedSize):
                    row['size'] = len(media.bytes)
                if isinstance(media.location, types.FileLocation):
                    media = media.location

        if isinstance(media, (types.UserProfilePhoto, types.ChatPhoto)):
            row['type'] = 'photo'
            row['mime_type'] = 'image/jpeg'
            row['thumbnail_id'] = await self.dump_media(
                media.photo_small, 'thumbnail'
            )
            media = media.photo_big

        if isinstance(media, types.FileLocation):
            row['local_id'] = media.local_id
            row['volume_id'] = media.volume_id
            row['secret'] = media.secret

        if row['type']:
            # We'll say two files are the same if they point to the same
            # downloadable content (through local_id/volume_id/secret).

            for callback in self._dump_callbacks['media']:
                callback(row)

            ret = await db_media.find_one({
                'local_id': row['local_id'],
                'volume_id': row['volume_id'],
                'secret': row['secret']
            })
            if ret:
                return ret['_id']
            return (await db_media.insert_one(row)).inserted_id

    async def dump_forward(self, forward):
        """
        Dump a message forward relationship into the Forward table.

        Params: MessageFwdHeader Telethon object
        Returns: ID of inserted row"""
        if not forward:
            return None

        row = {
            'id': forward.date.timestamp(),
            'original_date': forward.from_id,
            'from_id': forward.channel_post,
            'channel_post': forward.post_author
        }

        for callback in self._dump_callbacks['forward']:
            callback(row)
        ret = await db_forward.insert_one(row)
        return ret.inserted_id

    async def get_max_message_id(self, context_id):
        """
        Returns the largest saved message ID for the given
        context_id, or 0 if no messages have been saved.
        """
        ret = await db_message.find_one({'context_id': context_id}, sort=[('id', -1)])
        return ret['id']

    async def get_message_count(self, context_id):
        """Gets the message count for the given context"""
        return await db_message.count_documents({'context_id': context_id})

    async def get_resume(self, context_id):
        """
        For the given context ID, return a tuple consisting of the offset
        ID and offset date from which to continue, as well as at which ID
        to stop.
        """
        return await db_resume.find_one({'context_id': context_id}) or {'id': 0, 'date': 0, 'stop_at': 0}

    async def save_resume(self, context_id, msg=0, msg_date=0, stop_at=0):
        """
        Saves the information required to resume a download later.
        """
        if isinstance(msg_date, datetime):
            msg_date = int(msg_date.timestamp())

        return (await db_resume.insert_one(
            {'context_id': context_id, 'id': msg, 'date': msg_date, 'stop_at': stop_at})).inserted_id

    async def get_resume_entities(self, context_id):
        """
        Returns an iterator over the entities that need resuming for the
        given context_id. Note that the entities are *removed* once the
        iterator is consumed completely.
        """
        rows = db_resume_entity.find({'context_id': context_id})
        count = await db_resume_entity.count_documents({'context_id': context_id})
        logger.info(f'加载了【{count}】条待恢复数据')
        result = []
        async for row in rows:
            kind = resolve_id(row['id'])[1]
            if kind == types.PeerUser:
                result.append(types.InputPeerUser(row['id'], row['access_hash']))
            elif kind == types.PeerChat:
                result.append(types.InputPeerChat(row['id']))
            elif kind == types.PeerChannel:
                result.append(types.InputPeerChannel(row['id'], row['access_hash']))
        await db_resume_entity.delete_many({'context_id': context_id})
        return result

    async def save_resume_entities(self, context_id, entities):
        """
        Saves the given entities for resuming at a later point.
        """
        rows = []
        for ent in entities:
            ent = get_input_peer(ent)
            if isinstance(ent, types.InputPeerUser):
                rows.append({'context_id': context_id, 'id': ent.user_id, 'access_hash': ent.access_hash})
            elif isinstance(ent, types.InputPeerChat):
                rows.append({'context_id': context_id, 'id': ent.chat_id, 'access_hash': None})
            elif isinstance(ent, types.InputPeerChannel):
                rows.append({'context_id': context_id, 'id': ent.channel_id, 'access_hash': ent.access_hash})
        if rows:
            await db_resume_entity.insert_many(rows)

    async def iter_resume_media(self, context_id):
        """
        Returns an iterator over the media tuples that need resuming for
        the given context_id. Note that the media rows are *removed* once
        the iterator is consumed completely.
        """
        ret = db_resume_media.find({'context_id': context_id})
        async for row in ret:
            media_id, sender_id, date = row['media_id'], row['sender_id'], row['date']
            yield media_id, sender_id, datetime.utcfromtimestamp(date)
        await db_resume_media.delete_many({'context_id': context_id})

    async def save_resume_media(self, media_tuples):
        """
        Saves the given media tuples for resuming at a later point.

        The tuples should consist of four elements, these being
        ``(media_id, context_id, sender_id, date)``.
        """
        requests = []
        for row in media_tuples:
            _row = {
                'media_id': row[0],
                'content_id': row[1],
                'sender_id': row[2],
                'date': row[3]
            }
            requests.append(UpdateOne({'media_id': row[0]}, {'$set': _row}, upsert=True))
        if requests:
            await db_resume_media.bulk_write(requests)
