#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: kafka_producer.py
@time: 2016/12/19 16:32
"""
import sys

import pymongo
import time

sys.path.append('../')
from common import util
from common.mongo import MongDb
from logger import Gsxtlogger


log = Gsxtlogger('company_name_repair.log').get_logger()

province_zh_to_py = {
    '上海': 'shanghai',
    '云南': 'yunnan',
    '内蒙古': 'neimenggu',
    '北京': 'beijing',
    '吉林': 'jilin',
    '四川': 'sichuan',
    '天津': 'tianjin',
    '宁夏': 'ningxia',
    '安徽': 'anhui',
    '山东': 'shandong',
    '山西': 'shanxicu',
    '广东': 'guangdong',
    '广西': 'guangxi',
    '新疆': 'xinjiang',
    '江苏': 'jiangsu',
    '江西': 'jiangxi',
    '河北': 'hebei',
    '河南': 'henan',
    '浙江': 'zhejiang',
    '海南': 'hainan',
    '湖北': 'hubei',
    '湖南': 'hunan',
    '甘肃': 'gansu',
    '福建': 'fujian',
    '西藏': 'xizang',
    '贵州': 'guizhou',
    '辽宁': 'liaoning',
    '重庆': 'chongqing',
    '陕西': 'shanxi',
    '青海': 'qinghai',
    '黑龙江': 'heilongjiang',
    '上海市': 'shanghai',
    '云南省': 'yunnan',
    '北京市': 'beijing',
    '吉林省': 'jilin',
    '四川省': 'sichuan',
    '天津市': 'tianjin',
    '宁夏省': 'ningxia',
    '安徽省': 'anhui',
    '山东省': 'shandong',
    '山西省': 'shanxicu',
    '广东省': 'guangdong',
    '广西省': 'guangxi',
    '新疆省': 'xinjiang',
    '江苏省': 'jiangsu',
    '江西省': 'jiangxi',
    '河北省': 'hebei',
    '河南省': 'henan',
    '浙江省': 'zhejiang',
    '海南省': 'hainan',
    '湖北省': 'hubei',
    '湖南省': 'hunan',
    '甘肃省': 'gansu',
    '福建省': 'fujian',
    '贵州省': 'guizhou',
    '辽宁省': 'liaoning',
    '重庆市': 'chongqing',
    '陕西省': 'shanxi',
    '总局': 'gsxt'
}

mongo_db_company_data = {
    'host': '172.16.215.2',
    'port': 40042,
    'db': 'company_data',
    'username': 'work',
    'password': 'haizhi'
}

source_db = MongDb(mongo_db_company_data['host'], mongo_db_company_data['port'], mongo_db_company_data['db'],
                   mongo_db_company_data['username'], mongo_db_company_data['password'], log=log)

db_query = pymongo.MongoClient('172.16.215.2', 40042)['schedule_data']
db_query.authenticate('work', 'haizhi')


db_query_company_data = pymongo.MongoClient('172.16.215.2', 40042)['company_data']
db_query_company_data.authenticate('work', 'haizhi')

def main():
    try:
        count = 0
        all_count=0
        log.info('开始读取数据...')
        source_table = 'offline_all_list'
        cursor=db_query['offline_crawl_data_registration_company'].find({})
        result_list = list()
        for element in cursor:
            try:
                all_count += 1
                company_name = element['company']
                if ' ' in company_name:
                    query_result=db_query_company_data['offline_all_list'].find_one({'company_name':company_name})
                    if query_result:
                        db_query_company_data['offline_all_list'].delete_one({'company_name': company_name})
                else:
                    continue
                province=province_zh_to_py.get(str(element['province']),'')
                if province=='':
                    log.info("province error: {}".format(element['_id']))
                    continue
                count += 1
                company_name=str(company_name).strip()
                data = {
                    '_id': util.generator_id({}, company_name, province),
                    'company_name': str(company_name).strip(),
                    'province': province,
                    'in_time': util.get_now_time(),
                    'crawl_online': 0,
                }
                mod_value = {
                    'deal_flag': 1
                }
                db_query['offline_crawl_data_registration_company'].update({'_id': element['_id']}, {"$set": mod_value})
                log.info("已处理数据量: {}".format(all_count))
                result_list.append(data)
                if len(result_list) >= 100:
                    source_db.insert_batch_data(source_table, result_list)
                    del result_list[:]
            except Exception,e:
                print e.message
        source_db.insert_batch_data(source_table, result_list)
        log.info("总共发送数据: {}".format(count))
        log.info('数据发送完毕, 退出程序')
        time.sleep(100)
    except Exception,e:
        log.info(e.message)
        time.sleep(10)


if __name__ == '__main__':
    main()
