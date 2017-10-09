# coding=utf-8
__author__ = 'fengoupeng'

import logging
import os
from logging.handlers import TimedRotatingFileHandler

LOG_DIRECTORY = '/var/tmp/'
CRAWLER_PATH = os.getenv('CRAWLER_PATH')
if CRAWLER_PATH:
    LOG_DIRECTORY = '/%s/logs/' % CRAWLER_PATH


def LogHandler(name, loglevel=logging.INFO):
    LOG_FILE = LOG_DIRECTORY + "%s.log" % name
    fmt = "%(asctime)s - %(threadName)s - %(filename)s[line:%(lineno)d] %(levelname)s - %(message)s"
    formatter = logging.Formatter(fmt)
    handler = TimedRotatingFileHandler(LOG_FILE, when='MIDNIGHT', interval=1, backupCount=5)
    handler.setFormatter(formatter)

    # 控制台输出
    stream = logging.StreamHandler()
    stream.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.addHandler(handler)
    logger.addHandler(stream)
    logger.setLevel(loglevel)
    return logger
