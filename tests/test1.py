#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: test1.py
@time: 2017/7/23 00:18
"""

# 不使用代理代打开ip138
import random

import requests
from selenium import webdriver
from selenium.webdriver.common.proxy import ProxyType


def get_proxy_list():
    url = 'http://112.74.163.187:23128/__static__/proxies.txt'

    r = requests.get(url)
    if r.status_code != 200:
        return list()

    return r.text.split("\n")


browser = webdriver.PhantomJS(executable_path='../bin/mac/phantomjs')
browser.get('http://1212.ip138.com/ic.asp')
print('1: ', browser.session_id)
print('2: ', browser.page_source)
print('3: ', browser.get_cookies())

# 利用DesiredCapabilities(代理设置)参数值，重新打开一个sessionId，我看意思就相当于浏览器清空缓存后，加上代理重新访问一次url
proxy = webdriver.Proxy()
proxy.proxy_type = ProxyType.MANUAL
http_proxy = random.choice(get_proxy_list())
print http_proxy
proxy.http_proxy = http_proxy
# 将代理设置添加到webdriver.DesiredCapabilities.PHANTOMJS中
proxy.add_to_capabilities(webdriver.DesiredCapabilities.PHANTOMJS)
browser.start_session(webdriver.DesiredCapabilities.PHANTOMJS)
browser.get('http://1212.ip138.com/ic.asp')
print('1: ', browser.session_id)
print('2: ', browser.page_source)
print('3: ', browser.get_cookies())

# 还原为系统代理
proxy = webdriver.Proxy()
proxy.proxy_type = ProxyType.DIRECT
proxy.add_to_capabilities(webdriver.DesiredCapabilities.PHANTOMJS)
browser.start_session(webdriver.DesiredCapabilities.PHANTOMJS)
browser.get('http://1212.ip138.com/ic.asp')
print('1: ', browser.session_id)
print('2: ', browser.page_source)
print('3: ', browser.get_cookies())