"""A class to iterate through dialogs and dump them, or save past media"""

import re

from telegram_export.logger import create_logger
from .downloader import Downloader

logger = create_logger('exporter')


async def entities_from_str(method, string):
    """Helper function to load entities from the config file"""
    for who in string.split(','):
        if not who.strip():
            continue
        who = who.split(':', 1)[0].strip()  # Ignore anything after ':'
        if re.match(r'[^+]-?\d+', who):
            who = int(who)
        yield await method(who)


async def get_entities_iter(mode, in_list, client):
    """
    Get a generator of entities to act on given a mode ('blacklist',
    'whitelist') and an input from that mode. If whitelist, generator
    will be asynchronous.
    """
    # TODO change None to empty blacklist?
    mode = mode.lower()
    if mode == 'whitelist':
        assert client is not None
        async for ent in entities_from_str(client.get_input_entity, in_list):
            yield ent
    elif mode == 'blacklist':
        assert client is not None
        avoid = set()
        async for eid in entities_from_str(client.get_peer_id, in_list):
            avoid.add(eid)

        # TODO Should this get_dialogs call be cached? How?
        async for dialog in client.iter_dialogs():
            if dialog.id not in avoid:
                yield dialog.input_entity


class Exporter:
    """A class to iterate through dialogs and dump them, or save past media"""

    def __init__(self, client, config, dumper, loop):
        self.client = client
        self.config = config
        self.dumper = dumper
        self.downloader = Downloader(client, config['Dumper'], dumper, loop)
        self.loop = loop

    async def close(self):
        """Gracefully close the exporter"""
        # Downloader handles its own graceful exit
        logger.info("Closing exporter")
        await self.client.disconnect()
        logger.info("Finished!")

    async def start(self):
        """
        开始下载
        :return:
        """
        await self.dumper.check_self_user((await self.client.get_me(input_peer=True)).user_id)
        if 'Whitelist' in self.dumper.config:
            logger.info("使用白名单模式进行下载")
            async for entity in get_entities_iter('whitelist', self.dumper.config['Whitelist'], self.client):
                await self.downloader.start(entity)
        elif 'Blacklist' in self.dumper.config:
            logger.info("使用黑名单模式进行下载")
            async for entity in get_entities_iter('blacklist', self.dumper.config['Blacklist'], self.client):
                await self.downloader.start(entity)
        else:
            logger.info("我全都要")
            dialogs = await self.client.get_dialogs(limit=None)
            logger.info(f"获取了{len(dialogs)}个对话")
            for dialog in reversed(dialogs):
                await self.dumper.dump_dialog(dialog)
                await self.downloader.start(dialog.entity)

    async def download_past_media(self):
        """
        下载我们看到过但之前没有下载的文件
        :return:
        """
        await self.dumper.check_self_user((await self.client.get_me(input_peer=True)).user_id)
        if 'Whitelist' in self.dumper.config:
            logger.info("使用白名单模式进行下载")
            async for entity in get_entities_iter('whitelist', self.dumper.config['Whitelist'], self.client):
                await self.downloader.download_past_media(self.dumper, entity)
        elif 'Blacklist' in self.dumper.config:
            logger.info("使用黑名单模式进行下载")
            async for entity in get_entities_iter('blacklist', self.dumper.config['Blacklist'], self.client):
                await self.downloader.download_past_media(self.dumper, entity)
        else:
            logger.info("我全都要")
            for dialog in await self.client.get_dialogs(limit=None):
                await self.downloader.download_past_media(self.dumper, dialog.entity)

    def info(self):
        return {
            'message': self.downloader.running_message.to_dict(),
            'entity': self.downloader.running_entity.to_dict(),
            'media': [{key: value.to_dict()} for key, value in self.downloader.running_media_list.items()]
        }
