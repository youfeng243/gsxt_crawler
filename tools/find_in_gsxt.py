#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: find_in_gsxt.py
@time: 2017/8/11 14:32
"""
import sys

import pandas

sys.path.append('../')
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
blank = u''
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
                continue

            province = result_item.get('province')
            if province is None:
                province = blank
            log.info("找到企业信息: {}".format(company))
            gsxt_find_file.write(province + u"\t\t\t" + company + u"\r\n")

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
