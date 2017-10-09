#!/usr/bin/Python
# -*- coding: utf-8 -*-
from download_proccessor import DownloaderProccessor
from i_util.thread_pool import ThreadPool
from i_util.heart_beat import HeartbeatThread
from i_util.input_thread import InputThread
from i_util.ProfilingUtil import profiler_creator


class DownloaderServer(object):
    def __init__(self, conf):
        self.log = conf.get('log')
        thread_locals = {'processor': (DownloaderProccessor, (self.log, conf)), 'profiler': (profiler_creator, ())}
        self.process_pool = ThreadPool(conf['local_server'].get('process_thread_num'), thread_locals)
        self.input_thread = InputThread(conf.get('beanstalk_conf'), self.log, self.process_pool)
        self.heartbeat_thread = HeartbeatThread('download', conf)
        self.log.info('初始化线程信息完成..')

    def start(self):
        self.input_thread.start()
        self.heartbeat_thread.start()
        self.log.info("start DownloadServer!")

    def stop(self, message):
        self.log.info("stop DownloadServer %s!" % message)
        self.input_thread.stop()

