#!/usr/bin/python
import sys

from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
from thrift.transport.TSocket import TSocket

#from i_extractor.extractor import Extractor

sys.path.append('..')
from i_util import str_dict
from downloader.i_crawler.i_scheduler import SchedulerService
from downloader.i_crawler.i_extractor import ExtractorService
# from bdp.i_crawler.i_processor import Process
#from bdp.i_crawler.i_processor.ttypes import Req as ProcessReq
from downloader.i_crawler.i_downloader.ttypes import DownLoadReq
from downloader.i_crawler.i_downloader import DownloadService
from downloader.i_crawler.i_entity_extractor import EntityExtractorService
from downloader.i_crawler.i_crawler_merge import CrawlerMergeService
from downloader.i_crawler.i_data_saver import DataSaverService


# k_thrift_timeout = 360 * 1000

class ThriftScheduler(object):
    def __init__(self, host='localhost', port=9091):
        transport = TSocket(host=host, port=port)
        self.transport = TTransport.TBufferedTransport(transport)
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = SchedulerService.Client(self.protocol)

    def schedule_tasks(self, index_task, item_tasks=[]):
        for task in [index_task] + item_tasks:
            str_dict(task)
        self.transport.open()
        try:
            status = self.client.schedule_tasks(index_task, item_tasks)
        finally:
            self.transport.close()
        return status

    def start_one_site_tasks(self, site):
        self.transport.open()
        try:
            status = self.client.start_one_site_tasks(str(site))
        finally:
            self.transport.close()
        return status

    def stop_one_site_tasks(self, site):
        self.transport.open()
        try:
            status = self.client.stop_one_site_tasks(str(site))
        finally:
            self.transport.close()
        return status

    def dispatch_task(self):
        self.transport.open()
        try:
            task = self.client.dispatch_task()
        finally:
            self.transport.close()
        return task

    def clear_one_site_cache(self, site):
        self.transport.open()
        try:
            status = self.client.clear_one_site_cache(str(site))
        finally:
            self.transport.close()

        return status

    def restart_seed(self, seed_id, site):
        self.transport.open()
        try:
            status = self.client.restart_seed(str(seed_id), str(site))
        finally:
            self.transport.close()

        return status

class ThriftDownloader(object):

    def __init__(self, host='101.201.102.37', port=12200):
        transport = TSocket(host=host, port=port)
        self.transport = TTransport.TBufferedTransport(transport)
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = DownloadService.Client(self.protocol)

    def download(self, url, req = None):
        rsp = None;
        self.transport.open()
        try:
            if req is None:
                req = DownLoadReq();
            if url != None:
                req.url = url;
            rsp = self.client.download(req)
        finally:
            self.transport.close()
        return rsp
    def commit_download_task(self, url, req = None):
        rsp = None;
        self.transport.open()
        try:
            if req is None:
                req = DownLoadReq();
            if url != None:
                req.url = url;
            rsp = self.client.commit_download_task(req)
        finally:
            self.transport.close()
        return rsp

class ThriftExtractor(object):

    #def __init__(self, host='localhost', port=9093):
    def __init__(self, host='localhost', port=12300):
        transport = TSocket(host=host, port=port)
        self.transport = TTransport.TBufferedTransport(transport)
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = ExtractorService.Client(self.protocol)
        #self.extractor = Extractor(extractor_conf)


    def extract(self, download_rsp):
        self.transport.open()
        #rsp = None
        try:
            rsp = self.client.extract(download_rsp)
        finally:
            self.transport.close()
        return rsp

    def reload_parser_config(self, parser_id):
        self.transport.open()
        try:
            rsp = self.client.reload_parser_config(parser_id)
        finally:
            self.transport.close()
        return rsp
    def save_parser_config(self, config_json):
        self.transport.open()
        try:
            rsp = self.client.save_parser_config(config_json)
        finally:
            self.transport.close()
        return vars(rsp)

class ThriftCrawlerMerge(object):
    def __init__(self, host='localhost', port=12400):
        transport = TSocket(host=host, port=port)
        self.transport = TTransport.TBufferedTransport(transport)
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = CrawlerMergeService.Client(self.protocol)

    def merge(self, page_parseinfo):
        rsp = None
        try:
            self.transport.open()
            rsp = self.client.merge(page_parseinfo)
        finally:
            self.transport.close()
        return rsp

    def select(self, site, url_format, limit, start=0, tube_name='download_rsp'):
        rsp = None
        try:
            self.transport.open()
            rsp = self.client.select(site, url_format, limit, start, tube_name)
        finally:
            self.transport.close()
        return rsp

    def select_one(self, url):
        rsp = None
        try:
            self.transport.open()
            rsp = self.client.select_one(url)
        finally:
            self.transport.close()
        return rsp

class ThriftEntityExtractor(object):
    def __init__(self, host='localhost', port=12510):
        transport = TSocket(host=host, port=port)
        self.transport = TTransport.TBufferedTransport(transport)
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = EntityExtractorService.Client(self.protocol)

    def entity_extract(self, req):
        self.transport.open()
        rsp = None
        try:
            rsp = self.client.entity_extract(req)
        finally:
            self.transport.close()
        return rsp

    def reload(self, topic_id):
        self.transport.open()
        rsp = None
        try:
            rsp = self.client.reload(topic_id)
        finally:
            self.transport.close()
        return rsp

    def add_extractor(self, extractor_info):
        self.transport.open()
        rsp = None
        try:
            rsp = self.client.add_extractor(extractor_info)
        finally:
            self.transport.close()
        return rsp

    def add_topic(self, topic_info):
        self.transport.open()
        rsp = None
        try:
            rsp = self.client.add_topic(topic_info)
        finally:
            self.transport.close()
        return rsp

class ThriftDataSaver(object):
    def __init__(self, host='localhost', port=12600):
        transport = TSocket(host=host, port=port)
        self.transport = TTransport.TBufferedTransport(transport)
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = DataSaverService.Client(self.protocol)
    def check_data(self, entity_extractor_info, do_save = True):
        self.transport.open()
        rsp = None
        try:
            rsp = self.client.check_data(entity_extractor_info)
        finally:
            self.transport.close()
        return rsp

    def reload(self,topic_id):
        self.transport.open()
        rsp = None
        try:
            rsp = self.client.reload(topic_id)
        finally:
            self.transport.close()
        return rsp


class ThriftProcessor(object):

    def __init__(self, host='localhost', port=9095):
        transport = TSocket(host=host, port=port)
        self.transport = TTransport.TBufferedTransport(transport)
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = Process.Client(self.protocol)

    def process(self, action_type, items=[], info={}):
        req = ProcessReq(action_type=action_type, items=items, info=info)
        str_dict(req)
        self.transport.open()
        try:
            rsp = self.client.process(req)
        finally:
            self.transport.close()
        return rsp
