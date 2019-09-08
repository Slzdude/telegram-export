import asyncio
import json as _json
import os
from contextlib import suppress
from functools import partial

import click
from sanic import Sanic
from sanic.response import json
from telethon import TelegramClient

from telegram_export.config import load_config
from telegram_export.dumper import Dumper
from telegram_export.exporter import Exporter
from telegram_export.logger import main_logger
from telegram_export.utils import parse_proxy_str

sanic = Sanic()

NO_USERNAME = '<no username>'

exporter = None


def gen_server_error_resp(message):
    return json({'message': message})


@sanic.route('/info')
def info(request):
    if not exporter:
        raise gen_server_error_resp('程序尚未初始化完成，请稍后重试')
    return json(exporter.info(), dumps=partial(_json.dumps, separators=(",", ":"), ensure_ascii=False, indent=2))


async def main(loop=None, **kwargs):
    """
    The main telegram-export program. Goes through the
    configured dialogs and dumps them into the database.
    """
    global exporter
    config = load_config(kwargs.get('config_file'))
    dumper = Dumper(config['Dumper'])

    proxy = kwargs.get('proxy') or dumper.config.get('Proxy')
    if proxy:
        proxy = parse_proxy_str(proxy)

    absolute_session_name = os.path.join(config['Dumper']['OutputDirectory'], config['TelegramAPI']['SessionName'])
    client = await (TelegramClient(
        absolute_session_name,
        config['TelegramAPI']['ApiId'],
        config['TelegramAPI']['ApiHash'],
        loop=loop,
        proxy=proxy
    ).start(
        config['TelegramAPI']['PhoneNumber'],
        password=config['TelegramAPI']['SecondFactorPassword'] if config.has_option(
            'TelegramAPI', 'SecondFactorPassword') else None))
    exporter = Exporter(client, config, dumper, loop)

    main_logger.info(f'下载文件存储目录：{config["Dumper"]["OutputDirectory"]}')
    try:
        if kwargs.get('past', None):
            main_logger.info('下载过往数据')
            await exporter.download_past_media()
        else:
            main_logger.info('下载最新数据')
            await exporter.start()
    except asyncio.CancelledError:
        # This should be triggered on KeyboardInterrupt's to prevent ugly
        # traceback from reaching the user. Important code that always
        # must run (such as the Downloader saving resume info) should go
        # in their respective `finally:` blocks to ensure it gets called.
        pass
    finally:
        await exporter.close()


@click.command(help='Download Telegram data into a database')
@click.option('-c', '--config-file', default='config.ini',
              help='specify a config file. Default config.ini')
@click.option('-t', '--past', type=bool, default=False,
              help='download past media instead of dumping '
                   'new data (files that were seen before '
                   'but not downloaded).')
@click.option('-p', '--proxy', type=str, default=None,
              help='set proxy string. '
                   'Examples: socks5://user:password@127.0.0.1:1080. '
                   'http://localhost:8080')
def entry(**kwargs):
    loop = asyncio.get_event_loop()
    try:
        # 优先启动Sanic异步网络服务器
        asyncio.ensure_future(sanic.create_server(return_asyncio_server=True), loop=loop)
        # 启动爬虫主程序
        loop.run_until_complete(asyncio.ensure_future(main(**kwargs, loop=loop), loop=loop))
    except KeyboardInterrupt:
        pass
    for task in asyncio.Task.all_tasks():
        task.cancel()
        # Now we should await task to execute it's cancellation.
        # Cancelled task raises asyncio.CancelledError that we can suppress:
        if hasattr(task._coro, '__name__') and task._coro.__name__ == 'main':
            continue
        with suppress(asyncio.CancelledError):
            loop.run_until_complete(task)
    loop.stop()
    loop.close()


if __name__ == '__main__':
    entry()
