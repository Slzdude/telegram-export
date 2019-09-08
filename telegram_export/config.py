import configparser
import logging
import os
import re

from telegram_export.logger import main_logger

defaults = {
    'SessionName': 'exporter',
    'OutputDirectory': '.',
    'MediaWhitelist': 'chatphoto, photo, sticker',
    'MaxSize': '1MB',
    'MinSize': '0KB',
    'LogLevel': 'INFO',
    'DBFileName': 'export',
    'InvalidationTime': '7200',
    'ChunkSize': '1000',
    'MaxChunks': '0',
    'LibraryLogLevel': 'WARNING',
    'MediaFilenameFmt': 'usermedia/{name}-{context_id}/{type}-{filename}'
}


def parse_file_size(size_str):
    """
    将文件系统的容量描述字符串转换成Byte数量
    :param size_str: 文件大小
    :return:
    """
    m = re.match(r'(\d+(?:\.\d*)?)\s*([kmg]?b)?', size_str, re.IGNORECASE)
    if not m:
        raise ValueError('Invalid file size given for MaxSize')
    size = int(float(m.group(1)) * {
        'B': 1024 ** 0,
        'KB': 1024 ** 1,
        'MB': 1024 ** 2,
        'GB': 1024 ** 3
    }.get((m.group(2) or 'MB').upper()))
    return size


def load_config(filename='config.ini'):
    """
    读取指定位置的配置文件并进行解析
    :param filename:
    :return:
    """
    if not os.path.isfile(filename):
        main_logger.warning('未指定配置文件或者配置文件不存在！')
        exit(1)

    # Load from file
    config = configparser.ConfigParser(defaults)
    config.read(filename)

    # Check logging level (let it raise on invalid)
    level = config['Dumper'].get('LogLevel').upper()
    main_logger.setLevel(getattr(logging, level))
    # Library loggers
    level = config['Dumper'].get('LibraryLogLevel').upper()
    telethon_logger = logging.getLogger('telethon')
    telethon_logger.setLevel(getattr(logging, level))

    # Convert relative paths and paths with ~
    config['Dumper']['OutputDirectory'] = os.path.abspath(os.path.expanduser(config['Dumper']['OutputDirectory']))
    os.makedirs(config['Dumper']['OutputDirectory'], exist_ok=True)

    # Convert minutes to seconds
    config['Dumper']['InvalidationTime'] = str(config['Dumper'].getint('InvalidationTime', 7200) * 60)

    # Convert size to bytes
    config['Dumper']['MinSize'] = str(parse_file_size(config['Dumper'].get('MinSize')))
    config['Dumper']['MaxSize'] = str(parse_file_size(config['Dumper'].get('MaxSize')))
    return config
