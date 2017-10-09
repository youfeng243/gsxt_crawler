#!/usr/bin/env sh
ps -ef | grep "downloader_smart.toml" | grep python | grep -v grep | awk '{print $2}'