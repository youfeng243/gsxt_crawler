#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: find_in_gsxt.py
@time: 2017/8/11 14:32
"""
import json
import sys

import pandas

sys.path.append('../')
from common.pybeanstalk import PyBeanstalk

from common.mongo import MongDb

from logger import Gsxtlogger

log = Gsxtlogger('find_in_gsxt.log').get_logger()

db_conf = {
    'host': '172.16.215.16',
    'port': 40042,
    'db': 'app_data',
    'username': 'work',
    'password': 'haizhi',
}

source_db = MongDb(db_conf['host'], db_conf['port'], db_conf['db'],
                   db_conf['username'], db_conf['password'], log=log)

beanstalk_consumer_conf = {'host': 'cs0.sz-internal.haizhi.com', 'port': 11400}
beanstalk = PyBeanstalk(beanstalk_consumer_conf['host'], beanstalk_consumer_conf['port'])

province_zh_to_py = {
    u'上海': 'shanghai',
    u'云南': 'yunnan',
    u'内蒙古': 'neimenggu',
    u'北京': 'beijing',
    u'吉林': 'jilin',
    u'四川': 'sichuan',
    u'天津': 'tianjin',
    u'宁夏': 'ningxia',
    u'安徽': 'anhui',
    u'山东': 'shandong',
    u'山西': 'shanxicu',
    u'广东': 'guangdong',
    u'广西': 'guangxi',
    u'新疆': 'xinjiang',
    u'江苏': 'jiangsu',
    u'江西': 'jiangxi',
    u'河北': 'hebei',
    u'河南': 'henan',
    u'浙江': 'zhejiang',
    u'海南': 'hainan',
    u'湖北': 'hubei',
    u'湖南': 'hunan',
    u'甘肃': 'gansu',
    u'福建': 'fujian',
    u'西藏': 'xizang',
    u'贵州': 'guizhou',
    u'辽宁': 'liaoning',
    u'重庆': 'chongqing',
    u'陕西': 'shanxi',
    u'青海': 'qinghai',
    u'黑龙江': 'heilongjiang',
    u'上海市': 'shanghai',
    u'云南省': 'yunnan',
    u'北京市': 'beijing',
    u'吉林省': 'jilin',
    u'四川省': 'sichuan',
    u'天津市': 'tianjin',
    u'宁夏省': 'ningxia',
    u'安徽省': 'anhui',
    u'山东省': 'shandong',
    u'山西省': 'shanxicu',
    u'广东省': 'guangdong',
    u'广西省': 'guangxi',
    u'新疆省': 'xinjiang',
    u'江苏省': 'jiangsu',
    u'江西省': 'jiangxi',
    u'河北省': 'hebei',
    u'河南省': 'henan',
    u'浙江省': 'zhejiang',
    u'海南省': 'hainan',
    u'湖北省': 'hubei',
    u'湖南省': 'hunan',
    u'甘肃省': 'gansu',
    u'福建省': 'fujian',
    u'贵州省': 'guizhou',
    u'辽宁省': 'liaoning',
    u'重庆市': 'chongqing',
    u'陕西省': 'shanxi',
    u'总局': 'gsxt'
}


def classify():
    with open("company_invalid.txt", "w") as invalid_file:
        with open("company_valid.txt", "w") as valid_file:
            with open("company_list.txt") as p_file:
                for line in p_file:
                    company = line.strip("\r").strip("\n").strip()
                    # if source_db.find_one("enterprise_data_gov", {"company": company}) is None:
                    #     log.warn("当前企业不存在: {}".format(company))
                    # else:
                    #     log.info("找到企业信息: {}".format(company))
                    if len(company) <= 15 or len(company) > 90:
                        log.error("企业信息不正确: {}".format(company))
                        invalid_file.write(company + "\r\n")
                    else:
                        valid_file.write(company + "\r\n")


no = u'------'
yes = u'是'
blank = ''
title_list = [u'企业名称', u'省份', u'数据库中是否有工商信息', u'数据库中是否有年报信息', u'年报中是否有电话号码', u'工商官网是否能搜到', u'工商官网是否有年报']


def fill(company, province=blank, gsxt=no, annual=no, phone=no):
    item = dict()
    item[title_list[0]] = company.decode("utf-8")
    item[title_list[1]] = province.decode("utf-8")
    item[title_list[2]] = gsxt
    item[title_list[3]] = annual
    item[title_list[4]] = phone
    item[title_list[5]] = blank
    item[title_list[6]] = blank
    return item


def send_to_beanstalk(province, company):
    if province == blank or province is None:
        province = u'总局'
        log.info("当前省份信息不正确，不发送消息队列: province = {} company = {}".format(province, company))

    if province not in province_zh_to_py:
        log.error("当前省份信息无法转换为英文: province = {} company = {}".format(province, company))
        return

    province = province_zh_to_py[province]
    tube = 'gs_{}_scheduler'.format(province)

    log.info("当前需要发送到消息队列的企业信息: province = {} company = {} tube = {}".format(
        province, company, tube))

    data = {
        'company_name': company,
        'province': province,
    }
    data_str = json.dumps(data)
    beanstalk.put(tube, data_str)


def statistics():
    find = 0
    not_find = 0
    annual_find = 0
    annual_not_find = 0
    total = 0

    gsxt_find_file = open("找到工商信息企业名单.txt", "w")
    gsxt_not_find_file = open("没有找到工商信息企业名单.txt", "w")
    annual_find_file = open("找到工商和年报信息的企业名单.txt", "w")

    sheet_list = []

    excel_name = '工商企业名单.xls'
    with open("company_valid.txt") as p_file:
        for line in p_file:
            total += 1
            company = line.strip("\r").strip("\n").strip()
            result_item = source_db.find_one("enterprise_data_gov", {"company": company})
            if result_item is None:
                log.warn("当前企业不存在: {}".format(company))
                not_find += 1
                gsxt_not_find_file.write(company + "\r\n")
                sheet_list.append(fill(company))

                # 不存在的企业发送到总局
                send_to_beanstalk(blank, company)
                continue

            province = result_item.get('province')
            if province is None:
                province = blank
            log.info("找到企业信息: {}".format(company))
            gsxt_find_file.write(province + u"\t\t\t" + company + u"\r\n")

            # 发送到消息队列
            send_to_beanstalk(province, company)

            find += 1
            annual_item = source_db.find_one("annual_reports", {"company": company})
            if annual_item is None:
                annual_not_find += 1
                log.info("没有找到年报信息: {}".format(company))
                sheet_list.append(fill(company, province, yes))
                continue

            annual_find += 1
            log.info("找到年报信息: {}".format(company))
            if 'contact_number' in annual_item:
                sheet_list.append(fill(company, province, yes, yes, yes))
            else:
                sheet_list.append(fill(company, province, yes, yes))
            annual_find_file.write(province + u"\t\t\t" + company + u"\r\n")

    df = pandas.DataFrame(sheet_list, columns=title_list)
    with pandas.ExcelWriter(excel_name) as writer:
        df.to_excel(writer, index=False)
    log.info("总共统计的企业数目: {}".format(total))
    log.info("总共找到工商数目为: {}".format(find))
    log.info("总共找到年报数目为: {}".format(annual_find))
    log.info("总共没有找到年报数目为: {}".format(annual_not_find))
    log.info("总共没有找到工商数目为: {}".format(not_find))


def main():
    statistics()


if __name__ == '__main__':
    main()
