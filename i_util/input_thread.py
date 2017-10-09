# coding=utf-8
import sys
import threading
import time

from beanstalkc import SocketError

from i_util.logs import LogHandler
from i_util.pybeanstalk import PyBeanstalk

sys.path.append('..')


class InputThread(threading.Thread):
    def __init__(self, beanstalk_conf, log=None, process_pool=None):
        threading.Thread.__init__(self)
        self.daemon = True
        self.running = True

        assert beanstalk_conf is not None
        assert log is not None
        assert process_pool is not None

        self.beanstalk = PyBeanstalk(beanstalk_conf['host'], beanstalk_conf['port'])
        self.out_beanstalk = PyBeanstalk(beanstalk_conf['host'], beanstalk_conf['port'])
        self.input_tube = beanstalk_conf['input_tube']
        self.output_tube = beanstalk_conf['output_tube']
        self.log = log
        if not self.log:
            self.log = LogHandler("i_input_thread")

        self.process_pool = process_pool
        self.t_lock = threading.Lock()

    def stop(self):
        self.log.warning("stop input_thread")
        self.running = False
        try:
            while True:
                if self.process_pool.get_task_num() <= 0:
                    # if 'processor' in self.process_pool.thread_local_constructors:
                    #     processor = self.process_pool.thread_local_constructors['processor'][1][1]
                    #     self.log.warning("prepare call scheduler_processor to stop scheduler")
                    #     processor.save_status()
                    break
                else:
                    self.log.info("wait tasks be consumed over, wait 5s")
                    time.sleep(5)

            self.beanstalk.__del__()  # 关闭连接不再接受数据
        except Exception as e:
            self.log.error("stop input_thread fail")
            self.log.exception(e)

    def run(self):
        job_num = 0
        while self.running and self.input_tube:
            try:
                job = self.beanstalk.reserve(self.input_tube, 30)
                if job is not None:
                    job_num += 1
                    body = job.body
                    job.delete()

                    self.process_pool.queue_task(self.__on_task_start, (body,), self.__on_task_finished)
                    task_num = self.process_pool.get_task_num()
                    if task_num >= 50:
                        self.log.info("place_processor\ttasks:%d" % task_num)
                        time.sleep(2)
                else:
                    self.log.info("not msg from:%s" % self.input_tube)
            except SocketError as e:
                time.sleep(30)
                self.log.error('beanstalk\tconnect\tfail\tstart\treconnect')
                self.log.exception(e)
                try:
                    self.beanstalk.reconnect()
                    self.out_beanstalk.reconnect()
                    self.log.error('beanstalk\treconnect\tsuccess')
                except Exception as e:
                    self.log.error('beanstalk\treconnect\tfail')
                    self.log.exception(e)
            except Exception as e:
                self.log.error("not msg from:%s\tresult:" % self.input_tube)
                self.log.exception(e)

    @staticmethod
    def __on_task_start(task, **thread_locals):
        result = None
        if 'profiler' in thread_locals:
            thread_locals['profiler'].begin()
        if 'processor' in thread_locals:
            result = thread_locals['processor'].do_task(task)
        return result

    def __on_task_finished(self, (result), **thread_locals):
        self.t_lock.acquire()
        proccesor = None
        if 'processor' in thread_locals:
            proccesor = thread_locals['processor']
        if 'profiler' in thread_locals:
            thread_locals['profiler'].end()
        if result and isinstance(result, basestring):
            self.__output_msg(result, proccesor)
        elif isinstance(result, list):
            for message in result:
                self.__output_msg(message, proccesor)
        self.t_lock.release()

    def __output_msg(self, result_msg, proccesor):
        if result_msg and isinstance(result_msg, basestring):
            try:
                if isinstance(self.output_tube, list):
                    for output_tube in self.output_tube:
                        self.out_beanstalk.put(output_tube, str(result_msg))
                else:
                    self.out_beanstalk.put(self.output_tube, str(result_msg))
            except Exception as e:
                self.log.error("put msg from:%s\tresult:%s" % (self.output_tube, str(e)))
                self.log.exception(e)

        elif result_msg and proccesor:
            self.log.info("put_msg_to:%s\tresult:%s" % (self.output_tube, type(result_msg)))
            proccesor.do_output(result_msg)

# is_stop = False
# 
# 
# def signal_handler(signal=None, frame=None):
#     global is_stop
#     is_stop = True
# 
# 
# def main(beanstalk_conf, log=None, process_pool=None):
#     import signal
#     global is_stop
#     signal.signal(signal.SIGINT, signal_handler)
#     signal.signal(signal.SIGTERM, signal_handler)
#     signal.signal(signal.SIGQUIT, signal_handler)
#     signal.signal(signal.SIGUSR1, signal_handler)
#     tr = InputThread(beanstalk_conf)
#     tr.start()
#     while not is_stop:
#         time.sleep(10)
#     tr.stop()
# 
# 
# if __name__ == '__main__':
#     time.sleep(2)
#     beans_conf = dict()
#     # beans_conf['host'] = "101.201.102.37";
#     beans_conf['host'] = "127.0.0.1"
#     beans_conf['port'] = 11300
#     beans_conf['input_tube'] = 'scheduler_info'
#     beans_conf['output_tube'] = ['test1', 'test2']
#     main(beans_conf, log=None, process_pool=None)
