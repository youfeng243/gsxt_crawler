#!/bin/bash

source `dirname $0`/../env.sh

Prog=python
file_name="${CRAWLER_PATH}/downloader/server.py"
conf_name="${CRAWLER_PATH}/downloader/downloader_smart_test.toml"

start() {
	status
	[ $? -eq 1 ] && return 1
	echo $"Starting $file_name ... "
	nohup ${Prog} ${file_name} -f ${conf_name} > /dev/null 2>&1 &
	retval=$?
	${Prog} --version
	sleep 3
	status
	return ${retval}
}

stop() {
	status
	[ $? -eq 0 ] && return 0
	echo $"Stopping $file_name ... "
	sleep 1
	Pid=`ps -ef | grep ${file_name} | grep ${Prog} | grep -v grep | awk '{print $2}'`
	kill -9 ${Pid}
	# kill -9 `ps -ef | grep -v grep | grep phantomjs.*downloader | awk '{print $2}'`
    retval=$?
	status
	return ${retval}
}

restart() {
	stop
	start
}


status() {
	Pid=`ps -ef | grep ${file_name} | grep ${Prog} | grep -v grep | awk '{print $2}'`
	[ -z "$Pid" ] && echo "$file_name is not Running!" && return 0
	[ -n "$Pid" ] && echo "$Pid" && return 1
}


case "$1" in
	start|stop|restart|status)
  		$1
		;;
	*)
		echo $"Usage: $0 {start|stop|status|restart}"
		exit 1
esac

