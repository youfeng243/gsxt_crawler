#!/usr/bin/env bash
echo '更新最新代码...'
git pull

# 关掉脚本
ps -ef | grep -w start_offline_all_process.sh | grep -v grep | awk '{print $2}' | xargs kill -9

# 关掉程序
ps -ef | grep -w start_offline_crawler.py | grep -v grep | grep python | grep offline_gsxt_all | awk '{print $2}' | xargs kill -9
ps -ef | grep -i phantomjs | grep -v grep | grep shandong | awk '{print $2}' | xargs kill



echo '停止工商代理监控脚本'
echo '休眠5s'
inter_time=5
sleep ${inter_time}

# 启动全程抓取进程
nohup sh start_offline_all_process.sh > /dev/null 2>&1 &



echo '更新完成!'