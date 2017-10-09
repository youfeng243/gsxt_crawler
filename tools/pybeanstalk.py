# -*- coding: utf-8 -*-
import beanstalkc


class PyBeanstalk(object):
    def __init__(self, host, port=11400):
        self.host = host
        self.port = port
        self.__conn = beanstalkc.Connection(host, port)
        # self.__conn.watch('extract_info');
        # self.__conn.use('entity_info');

    def __del__(self):
        self.__conn.close()

    # beanstalk重连
    def reconnect(self):
        self.__conn.reconnect()

    def put(self, tube, body, priority=2 ** 31, delay=0, ttr=10):
        self.__conn.use(tube)
        if len(tube) >= 3145728:
            return None
        return self.__conn.put(body, priority, delay, ttr)

    def reserve(self, tube, timeout=20):
        # for t in self.__conn.watching():
        #    self.__conn.ignore(t)
        self.__conn.watch(tube)
        return self.__conn.reserve(timeout)

    def clear(self, tube):
        try:
            while 1:
                job = self.reserve(tube, 1)
                if job is None:
                    break
                else:
                    job.delete()
        except Exception, e:
            print e

    def stats_tube(self, tube):
        return self.__conn.stats_tube(tube)


if __name__ == '__main__':
    pybeanstalk = PyBeanstalk('cs0')
    # pybeanstalk.put('gs_scheduler', '{test2}')
    while True:
        job = pybeanstalk.reserve('gs_guangdong_shceduler')
        print job.body
