__author__ = 'fengoupeng'

import json
import os
import re
import sys
import time
import traceback

import redis

sys.path.append('..')

import tools
from logs import LogHandler
from pymysql import PyMySQL

logger = LogHandler('statistics')

def stat_fail_task():
    try:
        from i_scheduler.conf import redis_tasks
        redis_db = redis.Redis(host=redis_tasks['host'],
                               port=redis_tasks['port'],
                               db=redis_tasks['database'],
                               password=redis_tasks['password'],
                               )
        fail_tasks = redis_db.lrange(redis_tasks['key'], 0, -1)
        stat_dict = {}
        for fail_task in fail_tasks:
            json_task = json.loads(fail_task)
            site_id = json_task.get('site_id')
            doc_type = json_task.get('doc_type')
            download_type = json_task.get('download_type')
            fail_retry = json_task.get('fail_retry', '0')
            page_num = int(json_task.get('page_num'))
            key = site_id + '|' + doc_type + '|' + download_type + '|' + fail_retry
            if stat_dict.has_key(key):
                stat_dict[key][0] += 1
                stat_dict[key][1] += page_num
            else:
                stat_dict[key] = [1, page_num]
        line = 'STATISTICS FAIL TASKS : '
        site_dict = _load_site_dict()
        keys = stat_dict.keys()
        keys.sort()
        for key in keys:
            array = key.split('|')
            site_id = int(array[0])
            doc_type = array[1]
            download_type = array[2]
            fail_retry = array[3]
            fail_count = stat_dict[key][0]
            page_num = stat_dict[key][1]
            site_name = site_dict[site_id][0]
            seed_count = site_dict[site_id][1]
            if doc_type == 'item':
                line += '\r\n' + str(site_id) + '\t' + str(site_name) + '\t' + str(fail_retry) + '\t' + str(fail_count) + '\t' + str(doc_type) + '\t' + str(download_type)
            else:
                page_per_doc = page_num / float(fail_count)
                fail_rate = fail_count / float(seed_count)
                line += '\r\n' + str(site_id) + '\t' + str(site_name) + '\t' + str(fail_retry) + '\t' + str(fail_count) + '\t' + str(doc_type) + '\t' + str(download_type) + '\t' + str(page_per_doc) + '\t' + str(fail_rate)
        logger.info(line)
    except:
        logger.error('_schedule_tasks error : ' + str(traceback.format_exc()))

def _load_site_dict():
    from i_configurator.conf import mysql_host, mysql_port, database, username, password
    from i_configurator.loader_seeds import sites_param
    site_dict = {}
    mysql = PyMySQL(mysql_host, mysql_port, database, username, password)
    for item in sites_param:
        try:
            site_id = item.get('id')
            name = item.get('name')
            data = mysql.fetch('SELECT count(*) FROM seeds WHERE mode="on" and site_id=%d' % site_id, mysql.FETCH_ONE)
            count = data[0]
            site_dict[site_id] = [name, count]
        except:
            logger.error(str(item))
            logger.error('_load_site_dict error : ' + str(traceback.format_exc()))
    return site_dict

def stat_product_num(date):
    if not date:
        date = tools.get_date_bias(1)
    logger.info('STATISTICS PRODUCT NUMBER START.')
    logger.info('DATE : ' + str(date))
    product_num = {}
    from i_processor.conf import mysql_host, mysql_port, database, username, password
    db_1 = PyMySQL(mysql_host, mysql_port, database, username, password)
    product_num = _compute_product_num(db_1, date, product_num)
    from i_processor.conf import mysql_host_2, mysql_port_2, database_2, username_2, password_2
    db_2 = PyMySQL(mysql_host_2, mysql_port_2, database_2, username_2, password_2)
    product_num = _compute_product_num(db_2, date, product_num)
    db_2._close()
    from i_processor.conf import mysql_host_3, mysql_port_3, database_3, username_3, password_3
    db_3 = PyMySQL(mysql_host_3, mysql_port_3, database_3, username_3, password_3)
    product_num = _compute_product_num(db_3, date, product_num)
    db_3._close()
    from i_processor.conf import mysql_host_4, mysql_port_4, database_4, username_4, password_4
    db_4 = PyMySQL(mysql_host_4, mysql_port_4, database_4, username_4, password_4)
    product_num = _compute_product_num(db_4, date, product_num)
    db_4._close()
    product_num['create_time'] = int(time.time())
    logger.info('DATA DICT : ' + str(product_num))
    db_1.upsert('stat_product', {'date': date}, product_num)
    db_1._close()
    logger.info('STATISTICS PRODUCT NUMBER FINISH.')

def _compute_product_num(db, today, product_num):
    line = 'COMPUTE DATABASE PRODUCT : '
    line += '\r\nDATABASE : ' + str(db.host) + ' ' +  str(db.post)
    tables = db.fetch('show tables', PyMySQL.FETCH_ALL)
    for table in tables:
        if not table[0].endswith(today):
            continue
        array = table[0].split('_')
        if array[0] == 'ymt':
            continue
        line += '\r\nTABLE : ' + str(table[0])
        id_type = array[1]
        if not id_type in ['product', 'video']:
            id_type = 'product'
        sql = 'select count(%s_id), count(distinct %s_id) from %s' % (id_type, id_type, table[0])
        data = db.fetch(sql, PyMySQL.FETCH_ALL)
        if data:
            line += '\tDATA : ' + str(data[0])
            product_num[array[0] + '_' + array[1] + '_all'] = data[0][0]
            product_num[array[0] + '_' + array[1] + '_unq'] = data[0][1]
        else:
            line += '\tDATA : no data'
    logger.info(line)
    return product_num

def stat_download(date):
    try:
        logger.info('STATISTICS DOWNLOAD START.')
        if not date:
            date = tools.get_date_bias(1, formater='%Y-%m-%d')
        elif re.match('\d{8}', date):
            date = date[0:4] + '-' + date[4:6] + '-' + date[6:8]
        elif re.match('\d{4}-\d{2}-\d{2}', date):
            logger.error('PARAMETER ERROR : ' + str(date))
            return
        logger.info('DATE : ' + str(date))
        from i_util import LOG_DIRECTORY
        if date == tools.get_date_bias(0, formater='%Y-%m-%d'):
            filename = LOG_DIRECTORY + 'engine.log'
        else:
            filename = LOG_DIRECTORY + 'engine.log.' + date
        if not os.path.exists(filename):
            logger.error('FILE NOT EXISTS : ' + str(filename))
            return
        results = {}
        fileread = open(filename, 'r')
        for line in fileread:
            if line.find('_download_finished') == -1:
                continue
            array = line.split(':')
            hour = array[0]
            site_id = array[5].strip().split(' ')[0]
            type = array[6].strip().split(' ')[0]
            status = array[7].strip().split(' ')[0]
            key = hour + '|' + site_id + '|' + type + '|' + status
            if results.has_key(key):
                results[key] += 1
            else:
                results[key] = 1
        fileread.close()
        all_dict = {}
        hour_dict = {}
        site_dict = {}
        type_dict = {}
        keys = results.keys()
        keys.sort()
        for key in keys:
            array = key.split('|')
            hour = array[0]
            site_id = int(array[1])
            type = array[2]
            status = array[3]
            count = results[key]
            all_dict = _compute_dict(all_dict, 'all', status, count)
            hour_dict = _compute_dict(hour_dict, hour, status, count)
            site_dict = _compute_dict(site_dict, site_id, status, count)
            type_dict = _compute_dict(type_dict, type, status, count)
        _print_dict(all_dict)
        _print_dict(hour_dict)
        _print_dict(site_dict)
        _print_dict(type_dict)
        download = {}
        download = _insert_download(download, all_dict)
        download = _insert_download(download, type_dict)
        download['create_time'] = int(time.time())
        logger.info('DATA DICT : ' + str(download))
        from i_processor.conf import mysql_host, mysql_port, database, username, password
        mysql = PyMySQL(mysql_host, mysql_port, database, username, password)
        mysql.upsert('stat_download', {'date': date.replace('-', '')}, download)
        mysql._close()
    except:
        logger.error(str(traceback.format_exc()))
    finally:
        logger.info('STATISTICS DOWNLOAD FINISH.')

def _compute_dict(item_dict, item, status, count):
    if not item_dict.has_key(item):
        item_dict[item] = [0, 0, 0]
    temp = item_dict[item]
    if status == 'fail':
        temp[1] += count
        temp[2] += count
    else:
        temp[0] += count
        temp[2] += count
    item_dict[item] = temp
    return item_dict

def _print_dict(item_dict):
    keys = item_dict.keys()
    keys.sort
    for key in keys:
        logger.info(str(key) + ' ' + str(item_dict[key]))

def _insert_download(download, item_dict):
    keys = item_dict.keys()
    keys.sort
    for key in keys:
        item = item_dict[key]
        success = item[0]
        download[key + '_success'] = success
        fail = item[1]
        download[key + '_fail'] = fail
        count = item[2]
        download[key + '_count'] = count
    return download

if __name__ == '__main__':
    if len(sys.argv) > 1:
        if sys.argv[1] == 'stat_product_num':
            if len(sys.argv) > 2:
                date = sys.argv[2]
            else:
                date = None
            stat_product_num(date)
        elif sys.argv[1] == 'stat_fail_task':
            stat_fail_task()
        elif sys.argv[1] == 'stat_download':
            if len(sys.argv) > 2:
                date = sys.argv[2]
            else:
                date = None
            stat_download(date)
