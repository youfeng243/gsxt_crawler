#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: copy_data_to_beihai.py
@time: 2017/10/10 08:57
"""

import sys

sys.path.append('../')
from logger import Gsxtlogger

from common.mongo import MongDb

# 源数据表
mongo_db_source = {
    'host': '172.16.215.2',
    'port': 40042,
    'db': 'company_data',
    'username': 'work',
    'password': 'haizhi'
}

mongo_db_target = {
    'host': "103.36.136.211",
    'port': 40042,
    'db': 'company_data',
    "auth_db": "admin",
    "username": None,
    "password": None,
}

log = Gsxtlogger('copy_data_to_beihai.log').get_logger()
source_db = MongDb(mongo_db_source['host'], mongo_db_source['port'], mongo_db_source['db'],
                   mongo_db_source['username'], mongo_db_source['password'], log=log)

target_db = MongDb(mongo_db_target['host'], mongo_db_target['port'], mongo_db_target['db'],
                   mongo_db_target['username'], mongo_db_target['password'], log=log)


def main():
    collection_table = 'offline_all_list'

    log.info("开始导入数据..")
    result_list = []
    count = 0
    for item in source_db.traverse(collection_table):
        item['crawl_online'] = 0

        result_list.append(item)
        if len(result_list) >= 500:
            target_db.insert_batch_data(collection_table, result_list, insert=True)
            del result_list[:]
        count += 1
        if count % 1000 == 0:
            log.info("当前进度: count = {}".format(count))

    log.info("导入完成..")


if __name__ == '__main__':
    main()
