#!/usr/bin/Python
# -*- coding: utf-8 -*-

import signal
import sys

import time

sys.path.append('..')
import pytoml
import getopt
from DownloadService import DownloaderServer
from i_util.logs import LogHandler

is_running = True


def signal_process(signal, frame):
    global is_running
    logger.info('收到信号退出进程...')
    is_running = False


def main(conf):
    download_server = DownloaderServer(conf)
    download_server.start()

    logger.info('启动线程完成...')
    time.sleep(3)
    while is_running:
        # 判断进程是否或者
        if download_server.input_thread.is_alive:
            time.sleep(5)
            continue

        break
    download_server.stop('收到退出信息, 退出进程...')
    download_server.input_thread.join()


def usaget():
    pass


if __name__ == '__main__':
    try:
        # 增加信号处理..
        signal.signal(signal.SIGTERM, signal_process)
        signal.signal(signal.SIGINT, signal_process)
        signal.signal(signal.SIGQUIT, signal_process)
        # signal.signal(signal.SIGTERM, signal_process)
        # signal.signal(signal.SIGINT, signal_process)
        # signal.signal(signal.SIGQUIT, signal_process)
        # signal.signal(signal.SIGUSR1, lambda a, b: profiling_signal_handler("downloader", a, b))

        file_path = 'downloader_smart_test.toml'
        opt, args = getopt.getopt(sys.argv[1:], 'f:', ['help'])
        for name, value in opt:
            if name == "-f":
                file_path = value
            elif name in ("-h", "--help"):
                usaget()
                sys.exit()
            else:
                assert False, "unhandled option"

        if file_path is None or file_path.strip() == '':
            raise Exception('配置文件路径错误..')

        with open(file_path, 'rb') as fp:
            config = pytoml.load(fp)
        logger_name = config["local_server"].get('name') + str(config["local_server"].get('port'))
        logger = LogHandler(logger_name)
        config['log'] = logger
        logger.info(logger_name)
        logger.info('开始启动服务....')
        main(config)
    except getopt.GetoptError:
        raise Exception('参数读取错误...argv = {argv}'.format(argv=" ".join(sys.argv)))
