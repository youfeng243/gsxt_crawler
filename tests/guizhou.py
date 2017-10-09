#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: guizhou.py
@time: 2017/7/24 22:35
"""

import requests

from logger import Gsxtlogger

log = Gsxtlogger('guizhou.log').get_logger()


def get_http():
    url = 'http://112.74.163.187:23128/__static__/proxies.txt'
    r = requests.get(url)
    if r.status_code != 200:
        return []
    ip_list = r.text.split("\n")

    return ip_list


def main():
    ip_list = get_http()
    for ip in ip_list:

        url = 'http://gz.gsxt.gov.cn/2016/nzgsfgs/query!searchData.shtml'
        session = requests.Session()
        session.headers = {
            'Accept': 'application/json, text/javascript, */*',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6,zh-TW;q=0.4,ja;q=0.2',
            'Connection': 'keep-alive',
            'Content-Length': '77',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Cookie': 'UM_distinctid=15ab624b74d5ab-087147752ed8e6-1d3a6853-fa000-15ab624b74e452; Hm_lvt_cdb4bc83287f8c1282df45ed61c4eac9=1499133460,1499830591,1500100264,1500451692; _gscu_1078311511=00813999bxziyw80; _gscbrs_1078311511=1; JSESSIONID=3LSmZ2db1ppLBnyBfFsvv3Q1qGFnVzHVjTn5DvtX5hGFpkDnxQJ2!1578889163!-459977327; CNZZDATA2123887=cnzz_eid%3D1494418206-1483699047-%26ntime%3D1500941221; SERVERID=a8996613bfda32ff76278a7c0597f7d3|1500945971|1500945948',
            'DNT': '1',
            'Host': 'gz.gsxt.gov.cn',
            'Origin': 'http://gz.gsxt.gov.cn',
            'Referer': 'http://gz.gsxt.gov.cn/2016/nzgsfgs/jcxx_1.jsp?k=1b991b347fd59d9ca5bbafb531abab0df88edc803f93cc0819780e253130777b&ztlx=1&qylx=2190&a_iframe=jcxx_1',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest',
        }

        post_data = {
            'c': '0',
            'nbxh': '1b991b347fd59d9ca5bbafb531abab0df88edc803f93cc0819780e253130777b',
            't': '5',
        }

        session.proxies = {'http': 'http://{}'.format(ip)}

        try:
            r = session.post(url=url, data=post_data)
            if r is None:
                print '请求错误...'
                continue

            if r.status_code == 403:
                log.info(ip)
                continue

            print r.text
        except requests.exceptions.ProxyError as e:
            log.exception(e)


if __name__ == '__main__':
    main()
