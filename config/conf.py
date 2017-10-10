# -*- coding:utf-8 -*-

mongo_db_target = {
    "host": "172.17.1.119",
    "port": 40042,
    "db": "crawl_data",
    "username": 'work',
    "password": 'haizhi',
}

# 新网页库
# mongo_db_target_new = {
#     "host": "172.16.215.2",
#     "port": 40042,
#     "db": "crawl_data",
#     "username": "offline",
#     "password": "offline",
# }
#
# 新网页库
# mongo_db_target_new = {
#     "host": "172.17.1.119",
#     "port": 40042,
#     "db": "crawl_data_new",
#     "username": None,
#     "password": None,
# }

mongo_db_source = {
    'host': "172.17.1.119",
    'port': 40042,
    'db': 'company_data',
    "username": 'work',
    "password": 'haizhi',
}

# 代理服务
remote_proxy_conf = {
    'host': '112.74.163.187',
    'port': 9300,
}

# 新动态代理
# remote_proxy_conf_new = {
#     'host': '172.18.180.226',
#     'port': 9300,
# }

NGINX_IP = '172.17.1.117'

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
    'host': 'cs0.sz-internal.haizhi.com',
    'port': 11400,
}
# 解析消息队列
parse_mq_conf = {
    'host': 'cs0.sz-internal.haizhi.com',
    'port': 11400,
    'tube': 'online_gsxt_parse'
}
# 反馈消息队列
report_mq_conf = {
    'host': 'cs0.sz-internal.haizhi.com',
    'port': 11400,
    'tube': 'online_schedule_report'
}

# 静态代理访问链接
static_proxy_url = 'http://112.74.163.187:23128/__static__/proxies.txt'
