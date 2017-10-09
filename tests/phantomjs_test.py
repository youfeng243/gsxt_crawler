#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: phantomjs_test.py.py
@time: 2017/7/22 20:19
"""
# from selenium.webdriver.phantomjs import webdriver
#
#
# def main():
#     base_url = "http://sd.gsxt.gov.cn/"
#     driver = webdriver.WebDriver(executable_path='../bin/mac/phantomjs')
#     driver.get(base_url)
#
#
#     print driver.page_source
#     driver.close()
#     driver.quit()
#
#
# if __name__ == '__main__':
#     main()
import random
import time

import requests
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver import DesiredCapabilities

base_url = "http://sd.gsxt.gov.cn/"


def get_proxy_list():
    url = 'http://112.74.163.187:23128/__static__/proxies.txt'

    r = requests.get(url)
    if r.status_code != 200:
        return list()

    return r.text.split("\n")


proxy_list = get_proxy_list()

desired_capabilities = DesiredCapabilities.PHANTOMJS.copy()

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6,zh-TW;q=0.4,ja;q=0.2',
    'Cache-Control': 'max-age=0',
    'DNT': '1',
    'Host': 'sd.gsxt.gov.cn',
    'Proxy-Authorization': 'Basic Ympoejpiamh6',
    'Proxy-Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36',
}

for key, value in headers.iteritems():
    desired_capabilities['phantomjs.page.customHeaders.{}'.format(key)] = value


def get_phantomjs():
    service_args = ['--proxy={}'.format(random.choice(proxy_list)),
                    '--proxy-type=http',
                    '--load-images=no',
                    '--disk-cache=no',
                    '--ignore-ssl-errors=true',
                    '--cookies-file=cookies.txt']

    # proxy = webdriver.Proxy()
    # proxy.proxy_type = ProxyType.MANUAL
    # proxy.http_proxy = random.choice(proxy_list)
    # print proxy.http_proxy
    # proxy.add_to_capabilities(desired_capabilities)
    driver = webdriver.PhantomJS(executable_path='../bin/mac/phantomjs',
                                 desired_capabilities=desired_capabilities,
                                 service_args=service_args)

    driver.implicitly_wait(10)
    driver.set_page_load_timeout(10)
    driver.set_script_timeout(10)

    # driver.start_session()

    return driver


def full_init_phantomjs():
    driver = None
    try:
        driver = get_phantomjs()

        try:
            driver.get(base_url)
            print driver.page_source
            # print driver.get_cookies()
        except TimeoutException:
            driver.execute_script('window.stop()')

    except Exception as e:
        print e
    finally:
        if driver is not None:
            driver.close()
            driver.quit()


def optimize_phantomjs(d, url):
    try:
        # d.start_session(desired_capabilities)
        d.get(url)
        # print d.get_cookies()
        print d.page_source
        # print d.get_cookies()
    except TimeoutException:
        d.execute_script('window.stop()')
        # print d.page_source
        # print d.get_cookies()


def main():
    # start_time = time.time()
    # for _ in xrange(10):
    #     full_init_phantomjs()
    #     print _
    # end_time = time.time()
    #
    # print 'full_init_phantomjs 10次耗时: {} s'.format(end_time - start_time)

    driver = None
    start_time = time.time()
    try:

        driver = get_phantomjs()
        for _ in xrange(5):
            optimize_phantomjs(driver, base_url)
            # optimize_phantomjs(driver, 'http://www.jianshu.com/')
            print _

    except Exception as e:
        print e
    finally:
        if driver is not None:
            driver.close()
            driver.quit()
    end_time = time.time()

    print 'optimize_phantomjs 10次耗时: {} s'.format(end_time - start_time)


if __name__ == '__main__':
    main()
