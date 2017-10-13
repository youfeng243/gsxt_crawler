# -*- coding:utf-8 -*-

mongo_db_target = {
    "host": "Crawler-DataServer1",
    "port": 40042,
    "db": "crawl_data",
    "username": "work",
    "password": "haizhi",
}

mongo_db_source = {
    'host': "Crawler-DataServer1",
    'port': 40042,
    'db': 'company_data',
    'username': 'work',
    'password': 'haizhi'
}


NGINX_IP = 'Crawler-Geetest_crack_test'

# 滑动验证码破解服务
captcha_geetest_conf = {
    'url': 'http://{}:59876/api/crawl_scripts/gongshang'.format(NGINX_IP)
}

# 江西 重庆加密服务
encry_jx_cq_conf = {
    'url': 'http://{}:4000/api/run_script/gs_jx'.format(NGINX_IP)
}

# 浙江加密服务
encry_zj_conf = {
    'url': 'http://{}:4000/api/run_script/gs_zj'.format(NGINX_IP)
}

# 消息队列信息
beanstalk_consumer_conf = {
    'host': 'Crawler-Downloader2',
    'port': 11300,
}
# 解析消息队列
parse_mq_conf = {
    'host': 'Crawler-Downloader2',
    'port': 11300,
    'tube': 'online_gsxt_parse'
}
# 反馈消息队列
report_mq_conf = {
    'host': 'Crawler-Downloader2',
    'port': 11400,
    'tube': 'online_schedule_report'
}
