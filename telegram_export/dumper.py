"""A module for dumping export data into the database"""
import json
import logging
import sqlite3
import sys
import time
from base64 import b64encode
from datetime import datetime, timedelta
from enum import Enum
import os.path

import arrow
import pymongo
from pymongo import UpdateOne
from telethon.tl import types
from telethon.utils import get_peer_id, resolve_id, get_input_peer

from telegram_export.database import db, DB_SELFINFO, DB_ADMIN_LOG, DB_MESSAGE, DB_USER, DB_CHANNEL, DB_SUPERGROUP, \
    DB_CHATPARTICIPANTS, DB_MEDIA, DB_FORWARD, DB_RESUME, DB_RESUMEMEDIA, DB_RESUMEENTITY
from . import utils

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
        if 'DBFileName' in self.config:
            where = self.config["DBFileName"]
            if where != ':memory:':
                where = '{}.db'.format(os.path.join(
                    self.config['OutputDirectory'], self.config['DBFileName']
                ))
            self.conn = sqlite3.connect(where, check_same_thread=False)
        else:
            logger.error("A database filename is required!")
            exit()
        c = self.conn.cursor()

        self.chunk_size = max(int(config.get('ChunkSize', 100)), 1)
        self.max_chunks = max(int(config.get('MaxChunks', 0)), 0)
        self.invalidation_time = max(config.getint('InvalidationTime', 0), -1)

        self.dump_methods = ('message', 'user', 'message_service', 'channel',
                             'supergroup', 'chat', 'adminlog_event', 'media',
                             'participants_delta', 'media', 'forward')

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
        the database. The list of valid dump methods is dumper.dump_methods.
        If the dumper does not dump a row due to the invalidation_time, the
        callback will still be called.
        """
        if dump_method not in self.dump_methods:
            raise ValueError("Cannot attach callback to method {}. Available "
                             "methods are {}".format(dump_method, self.dump_methods))

        self._dump_callbacks[dump_method].add(callback)

    def remove_callback(self, dump_method, callback):
        """
        Remove the callback function from the set of callbacks for the given
        dump method. Will raise KeyError if the callback is not in the set of
        callbacks for that method
        """
        if dump_method not in self.dump_methods:
            raise ValueError("Cannot remove callback from method {}. Available "
                             "methods are {}".format(dump_method, self.dump_methods))

        self._dump_callbacks[dump_method].remove(callback)

    def check_self_user(self, self_id):
        """
        Checks the self ID. If there is a stored ID and it doesn't match the
        given one, an error message is printed and the application exits.
        """
        col = db.get_collection(DB_SELFINFO)
        ret = col.find_one()
        if ret and ret.get('user_id') != self_id:
            print('This export database belongs to another user!', file=sys.stderr)
            exit(1)
        col.update_one({'user_id': self_id}, {'$set': {'user_id': self_id}}, upsert=True)

    def dump_message(self, message, context_id, forward_id, media_id):
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

        row = (message.id,
               context_id,
               message.date.timestamp(),
               message.from_id,
               message.message,
               message.reply_to_msg_id,
               forward_id,
               message.post_author,
               message.views,
               media_id,
               utils.encode_msg_entities(message.entities),
               None)  # No MessageAction

        for callback in self._dump_callbacks['message']:
            callback(row)
        msg_col = db.get_collection(DB_MESSAGE)
        ret = msg_col.update_one({'id': row[0]}, {'$set': dict(zip([
            'id', 'context_id', 'date', 'from_id', 'message',
            'reply_message_id', 'forward_id', 'post_author', 'view_count',
            'media_id', 'formatting', 'service_action'
        ], row))}, upsert=True)
        return ret

    def dump_message_service(self, message, context_id, media_id):
        """Similar to self.dump_message, but for MessageAction's."""
        name = utils.action_to_name(message.action)
        if not name:
            return

        extra = message.action.to_dict()
        del extra['_']  # We don't need to store the type, already have name
        sanitize_dict(extra)
        extra = json.dumps(extra)

        row = (message.id,
               context_id,
               message.date.timestamp(),
               message.from_id,
               extra,  # Message field contains the information
               message.reply_to_msg_id,
               None,  # No forward
               None,  # No author
               None,  # No views
               media_id,  # Might have e.g. a new chat Photo
               None,  # No entities
               name)

        for callback in self._dump_callbacks['message_service']:
            callback(row)

        msg_col = db.get_collection(DB_MESSAGE)
        ret = msg_col.update_one({'id': row[0]}, {'$set': dict(zip([
            'id', 'context_id', 'date', 'from_id', 'message',
            'reply_message_id', 'forward_id', 'post_author', 'view_count',
            'media_id', 'formatting', 'service_action'
        ], row))}, upsert=True)
        return ret

    def dump_admin_log_event(self, event, context_id, media_id1, media_id2):
        """Similar to self.dump_message_service but for channel actions."""
        name = utils.action_to_name(event.action)
        if not name:
            return

        extra = event.action.to_dict()
        del extra['_']  # We don't need to store the type, already have name
        sanitize_dict(extra)
        extra = json.dumps(extra)

        row = (event.id, context_id, event.date.timestamp(), event.user_id,
               media_id1, media_id2, name, extra)

        for callback in self._dump_callbacks['adminlog_event']:
            callback(row)

        admin_log_col = db.get_collection(DB_ADMIN_LOG)
        ret = admin_log_col.update_one({'id': row[0]}, {'$set': dict(zip([
            'id', 'context_id', 'date', 'user_id',
            'media_id_1', 'media_id_2', 'action', 'data'
        ], row))}, upsert=True)
        return ret

    def dump_user(self, user_full, photo_id, timestamp=None):
        """Dump a UserFull into the User table
        Params: UserFull to dump, MediaID of the profile photo in the DB
        Returns -, or False if not added"""
        # Rationale for UserFull rather than User is to get bio
        row = (user_full.user.id,
               timestamp or round(time.time()),
               user_full.user.first_name,
               user_full.user.last_name,
               user_full.user.username,
               user_full.user.phone,
               user_full.about,
               user_full.user.bot,
               user_full.common_chats_count,
               photo_id)

        for callback in self._dump_callbacks['user']:
            callback(row)

        user_col = db.get_collection(DB_USER)
        ret = user_col.find_one({'id': row[0]})
        if not ret or time.time() - ret['date_updated'] > self.invalidation_time:
            ret = user_col.update_one({'id': row[0]}, {'$set': dict(zip([
                'id', 'date_updated', 'first_name', 'last_name', 'username',
                'phone', 'about', 'bot', 'common_chats_count', 'photo_id'
            ], row))}, upsert=True)
        return ret

    def dump_channel(self, channel_full, channel, photo_id, timestamp=None):
        """Dump a Channel into the Channel table.
        Params: ChannelFull, Channel to dump, MediaID
                of the profile photo in the DB
        Returns -"""
        # Need to get the full object too for 'about' info
        row = (get_peer_id(channel),
               timestamp or round(time.time()),
               channel_full.about,
               channel.title,
               channel.username,
               photo_id,
               channel_full.pinned_msg_id)

        for callback in self._dump_callbacks['channel']:
            callback(row)

        channel_col = db.get_collection(DB_CHANNEL)
        ret = channel_col.find_one({'id': row[0]})
        if not ret or time.time() - ret['date_updated'] > self.invalidation_time:
            ret = channel_col.update_one({'id': row[0]}, {'$set': dict(zip([
                'id', 'date_updated', 'about', 'title', 'username',
                'photo_id', 'pin_message_id'
            ], row))}, upsert=True)
        return ret

    def dump_supergroup(self, supergroup_full, supergroup, photo_id,
                        timestamp=None):
        """Dump a Supergroup into the Supergroup table
        Params: ChannelFull, Channel to dump, MediaID
                of the profile photo in the DB.
        Returns -"""
        # Need to get the full object too for 'about' info
        row = (get_peer_id(supergroup),
               timestamp or round(time.time()),
               getattr(supergroup_full, 'about', None) or '',
               supergroup.title,
               supergroup.username,
               photo_id,
               supergroup_full.pinned_msg_id)

        for callback in self._dump_callbacks['supergroup']:
            callback(row)

        supergroup_col = db.get_collection(DB_SUPERGROUP)
        ret = supergroup_col.find_one({'id': row[0]})
        if not ret or time.time() - ret['date_updated'] > self.invalidation_time:
            ret = supergroup_col.update_one({'id': row[0]}, {'$set': dict(zip([
                'id', 'date_updated', 'about', 'title', 'username',
                'photo_id', 'pin_message_id'
            ], row))}, upsert=True)
        return ret

    def dump_chat(self, chat, photo_id, timestamp=None):
        """Dump a Chat into the Chat table
        Params: Chat to dump, MediaID of the profile photo in the DB
        Returns -"""
        if isinstance(chat.migrated_to, types.InputChannel):
            migrated_to_id = chat.migrated_to.channel_id
        else:
            migrated_to_id = None

        row = (get_peer_id(chat),
               timestamp or round(time.time()),
               chat.title,
               migrated_to_id,
               photo_id)

        for callback in self._dump_callbacks['chat']:
            callback(row)

        chat_col = db.get_collection(DB_SUPERGROUP)
        ret = chat_col.find_one({'id': row[0]})
        if not ret or time.time() - ret['date_updated'] > self.invalidation_time:
            ret = chat_col.update_one({'id': row[0]}, {'$set': dict(zip([
                'id', 'date_updated', 'title', 'migrated_to_id', 'photo_id'
            ], row))}, upsert=True)
        return ret

    def dump_participants_delta(self, context_id, ids):
        """
        Dumps the delta between the last dump of IDs for the given context ID
        and the current input user IDs.
        """
        ids = set(ids)
        chat_party = db.get_collection(DB_CHATPARTICIPANTS)
        ret = chat_party.find({'context_id': context_id}).sort([('date_updated', pymongo.ASCENDING)])
        if not ret.count():
            added = ids
            removed = set()
        else:
            # Build the last known list of participants from the saved deltas
            last_ids = set(ret[0]['added'])
            for row in ret[1:]:
                last_ids = set(row['added'])
                added = set(row['added'])
                removed = set(row['removed'])
                last_ids = (last_ids | added) - removed
            added = ids - last_ids
            removed = last_ids - ids

        row = (context_id,
               round(time.time()),
               list(added),
               list(removed))

        for callback in self._dump_callbacks['participants_delta']:
            callback(row)

        chat_party.insert(dict(zip([
            'context_id', 'date_updated', 'added', 'removed'
        ], row)))
        return added, removed

    def dump_media(self, media, media_type=None):
        """Dump a MessageMedia into the Media table
        Params: media Telethon object
        Returns: ID of inserted row"""
        if not media:
            return

        row = {x: None for x in (
            'name', 'mime_type', 'size', 'thumbnail_id',
            'local_id', 'volume_id', 'secret'
        )}
        row['type'] = media_type
        row['extra'] = media.to_dict()
        sanitize_dict(row['extra'])
        row['extra'] = json.dumps(row['extra'])

        if isinstance(media, types.MessageMediaContact):
            row['type'] = 'contact'
            row['name'] = '{} {}'.format(media.first_name, media.last_name)
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
                row['thumbnail_id'] = self.dump_media(doc.thumb)
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
                row['thumbnail_id'] = self.dump_media(game.photo)
                row['local_id'] = game.id
                row['secret'] = game.access_hash

        elif isinstance(media, types.MessageMediaGeo):
            row['type'] = 'geo'
            geo = media.geo
            if isinstance(geo, types.GeoPoint):
                row['name'] = '({}, {})'.format(repr(geo.lat), repr(geo.long))

        elif isinstance(media, types.MessageMediaGeoLive):
            row['type'] = 'geolive'
            geo = media.geo
            if isinstance(geo, types.GeoPoint):
                row['name'] = '({}, {})'.format(repr(geo.lat), repr(geo.long))

        elif isinstance(media, types.MessageMediaInvoice):
            row['type'] = 'invoice'
            row['name'] = media.title
            row['thumbnail_id'] = self.dump_media(media.photo)

        elif isinstance(media, types.MessageMediaPhoto):
            row['type'] = 'photo'
            row['mime_type'] = 'image/jpeg'
            media = media.photo

        elif isinstance(media, types.MessageMediaUnsupported):
            row['type'] = 'unsupported'
            return

        elif isinstance(media, types.MessageMediaVenue):
            row['type'] = 'venue'
            row['name'] = '{} - {} ({}, {} {})'.format(
                media.title, media.address,
                media.provider, media.venue_id, media.venue_type
            )
            geo = media.geo
            if isinstance(geo, types.GeoPoint):
                row['name'] += ' at ({}, {})'.format(
                    repr(geo.lat), repr(geo.long)
                )

        elif isinstance(media, types.MessageMediaWebPage):
            row['type'] = 'webpage'
            web = media.webpage
            if isinstance(web, types.WebPage):
                row['name'] = web.title
                row['thumbnail_id'] = self.dump_media(web.photo, 'thumbnail')
                row['local_id'] = web.id
                row['secret'] = web.hash

        if isinstance(media, types.Photo):
            # Extra fallback cases for common parts
            row['type'] = 'photo'
            row['mime_type'] = 'image/jpeg'
            row['name'] = arrow.get(media.date).format('YYYYMMDDHHmmssSSS')
            sizes = [x for x in media.sizes
                     if isinstance(x, (types.PhotoSize, types.PhotoCachedSize))]
            if sizes:
                small = min(sizes, key=lambda s: s.w * s.h)
                large = max(sizes, key=lambda s: s.w * s.h)
                media = large
                if small != large:
                    row['thumbnail_id'] = self.dump_media(small, 'thumbnail')

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
            row['type'] = 'userphoto'
            row['mime_type'] = 'image/jpeg'
            row['thumbnail_id'] = self.dump_media(
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
            media_col = db.get_collection(DB_MEDIA)
            ret = media_col.find_one({
                'local_id': row['local_id'],
                'volume_id': row['volume_id'],
                'secret': row['secret']
            })
            if ret:
                return ret['_id']
            ret = media_col.insert(row)
            return ret

    def dump_forward(self, forward):
        """
        Dump a message forward relationship into the Forward table.

        Params: MessageFwdHeader Telethon object
        Returns: ID of inserted row"""
        if not forward:
            return None

        row = (forward.date.timestamp(),
               forward.from_id,
               forward.channel_post,
               forward.post_author)

        for callback in self._dump_callbacks['forward']:
            callback(row)
        ret = db[DB_FORWARD].insert(dict(zip(['id', 'original_date', 'from_id', 'channel_post', 'post_author'], row)))
        return ret

    def get_max_message_id(self, context_id):
        """
        Returns the largest saved message ID for the given
        context_id, or 0 if no messages have been saved.
        """
        ret = db[DB_MESSAGE].find_one({'context_id': context_id}, sort=[('id', -1)])
        return ret['id']

    def get_message_count(self, context_id):
        """Gets the message count for the given context"""
        return db[DB_MESSAGE].find({'context_id': context_id}).count()

    def get_resume(self, context_id):
        """
        For the given context ID, return a tuple consisting of the offset
        ID and offset date from which to continue, as well as at which ID
        to stop.
        """
        return db[DB_RESUME].find_one({'context_id': context_id}) or {'id': 0, 'date': 0, 'stop_at': 0}

    def save_resume(self, context_id, msg=0, msg_date=0, stop_at=0):
        """
        Saves the information required to resume a download later.
        """
        if isinstance(msg_date, datetime):
            msg_date = int(msg_date.timestamp())

        return db[DB_RESUME].insert({'context_id': context_id, 'id': msg, 'date': msg_date, 'stop_at': stop_at})

    def iter_resume_entities(self, context_id):
        """
        Returns an iterator over the entities that need resuming for the
        given context_id. Note that the entities are *removed* once the
        iterator is consumed completely.
        """
        rows = db[DB_RESUMEENTITY].find({'context_id': context_id})
        for row in rows:
            kind = resolve_id(row['id'])[1]
            if kind == types.PeerUser:
                yield types.InputPeerUser(row['id'], row['access_hash'])
            elif kind == types.PeerChat:
                yield types.InputPeerChat(row['id'])
            elif kind == types.PeerChannel:
                yield types.InputPeerChannel(row['id'], row['access_hash'])
        db[DB_RESUMEENTITY].delete_many({'context_id': context_id})

    def save_resume_entities(self, context_id, entities):
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
            db[DB_RESUMEENTITY].insert_many(rows)

    def iter_resume_media(self, context_id):
        """
        Returns an iterator over the media tuples that need resuming for
        the given context_id. Note that the media rows are *removed* once
        the iterator is consumed completely.
        """
        ret = db[DB_RESUMEMEDIA].find({'context_id': context_id})
        for row in ret:
            media_id, sender_id, date = row['media_id'], row['sender_id'], row['date']
            yield media_id, sender_id, datetime.utcfromtimestamp(date)
        db[DB_RESUMEMEDIA].delete_many({'context_id': context_id})

    def save_resume_media(self, media_tuples):
        """
        Saves the given media tuples for resuming at a later point.

        The tuples should consist of four elements, these being
        ``(media_id, context_id, sender_id, date)``.
        """
        requests = []
        for row in media_tuples:
            requests.append(UpdateOne({'media_id': row[0]}, {'$set': dict(zip([
                'media_id', 'content_id', 'sender_id', 'date'], row))}, upsert=True))
        if requests:
            db[DB_RESUMEMEDIA].bulk_write(requests)
