import logging
import os
from logging.handlers import TimedRotatingFileHandler

# 日志级别
CRITICAL = 50
FATAL = CRITICAL
ERROR = 40
WARNING = 30
WARN = WARNING
INFO = 20
DEBUG = 10
NOTSET = 0

LOG_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'logs')


def create_logger(name, level=INFO, stream=True, file=False):
    logger = logging.getLogger(name)
    if len(logger.handlers) < 1:
        formatter = logging.Formatter("[%(levelname)s] %(asctime)s [%(filename)s-%(lineno)d]: %(message)s")
        if stream:
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        if file:
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            handler = TimedRotatingFileHandler(os.path.join(base_dir, 'logs', f'{name}.log'), 'd', 1, 3)
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        logger.setLevel(level)
        logger.propagate = False
    return logger


main_logger = create_logger('Main', file=True)

if __name__ == '__main__':
    main_logger.info('this is a test msg')
