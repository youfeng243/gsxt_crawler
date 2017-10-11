#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: proxy_check.py
@time: 2017/10/12 00:24
"""
import json

import requests

from logger import Gsxtlogger

log = Gsxtlogger('proxy_check.log').get_logger()


def local_proxy():
    return 'test'


def get_proxy():
    proxy_url = 'http://101.132.128.78:18585/proxy'

    user_config = {
        'username': 'beihai',
        'password': 'beihai',
    }
    try:
        r = requests.post(proxy_url, json=user_config)
        if r.status_code != 200:
            return local_proxy()
        json_data = json.loads(r.text)
        is_success = json_data.get('success')
        if not is_success:
            return local_proxy()

        proxy = json_data.get('proxy')
        if proxy is None:
            return local_proxy()

        return {'http': proxy}
    except Exception as e:
        log.error('获取代理异常:')
        log.exception(e)

    return local_proxy()


def main():
    proxy = get_proxy()

    print proxy


if __name__ == '__main__':
    main()
