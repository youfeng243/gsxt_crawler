#!/usr/bin/Python
# coding=utf-8
import sys

from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport.TTransport import TMemoryBuffer

sys.path.append('..')
# from DownloaderHandle import DownloadHandler
# from handler import DownloadHandler as handler
from bdp.i_crawler.i_downloader.ttypes import DownLoadRsp
from bdp.i_crawler.i_downloader.ttypes import CrawlStatus
from i_util.normal_proccessor import NormalProccessor

import json
from util.gongshang_province_site_map import prov_site_map
import urllib
import requests
from i_util.tools import str_obj
from i_util.pybeanstalk import PyBeanstalk


class DownloaderProccessor(NormalProccessor):
    def __init__(self, log, conf):
        self.log = log
        self.conf = conf

        assert log is not None
        assert isinstance(conf, dict)

        self.type_extractor_map = self.conf['type_extractor_map']
        self.smart_proxy_url = self.conf['smart_proxy_url']

        self.out_beanstalk = PyBeanstalk(self.conf['beanstalk_conf']['host'], self.conf['beanstalk_conf']['port'])
        self.output_tube_scheduler = self.conf['beanstalk_conf']['output_tube_scheduler']

    def to_string(self, download_rsp):
        str_rsq = None
        try:
            t_memory_b = TMemoryBuffer()
            t_binary_protocol_b = TBinaryProtocol(t_memory_b)
            download_rsp.write(t_binary_protocol_b)
            str_rsq = t_memory_b.getvalue()
            # self.log.info('data-length is {}'.format(str(len(str_rsq))))
        except EOFError:
            self.log.warning("cann't write PageParseInfo to string")
        return str_rsq

    def do_task(self, body):
        try:
            download_req = json.loads(body)
            self.log.info("request_msg\t%s" % download_req)

            target_extractor_id = self.type_extractor_map[download_req['_type']]

            name = download_req['name'].encode('utf-8')
            target_url = 'http://%(site)s/gongshang_search?%(query)s' % {
                'site': prov_site_map[download_req['province']],
                'query': urllib.urlencode({
                    'name': name,
                    'original_query': json.dumps(download_req)
                })
            }
            self.log.info('请求代理企业名称: name = {name}'.format(name=name))

            response = requests.get(target_url, proxies={'http': self.smart_proxy_url})
            if response.status_code != 200:
                download_rsp = DownLoadRsp(status=CrawlStatus.CRAWL_FAILT, )
                return self.to_string(download_rsp)

            self.log.debug(response.text)

            resp_json = response.json()

            url = resp_json['url']

            # 组装DownloadRsp
            resp = dict()
            resp['url'] = str_obj(url)
            resp['download_time'] = resp_json.get('entitySrcDownloadTime', 0)
            resp['pages'] = []
            resp['content'] = str_obj(resp_json['html'])
            if resp['content'] is None:
                resp['content'] = '<html></html>'
            resp['data_extends'] = str_obj(json.dumps(resp_json['entity']))
            resp['parse_extends'] = str_obj(json.dumps({"parser_id": target_extractor_id}))
            resp['page_size'] = len(resp['content'])
            resp['content_type'] = 'text/html'
            resp['src_type'] = 'webpage'
            # resp['info'] = request.info
            # resp['scheduler'] = request.scheduler
            # resp['parse_extends'] = request.parse_extends
            resp['http_code'] = response.status_code
            resp['elapsed'] = int(response.elapsed.microseconds / 1000.0)
            resp['status'] = CrawlStatus.CRAWL_SUCCESS
            download_rsp = DownLoadRsp(**resp)

            self.log.info('发送到解析器的 name = {name} url = {url}'.format(name=name, url=resp['url']))

            # self.log.info(download_rsp)

            # 写给工商调度
            company_name = resp_json['entity'].get('company')
            self.out_beanstalk.put(self.output_tube_scheduler, json.dumps({
                'company': company_name,
                'crawl_online': resp_json['crawlStatus'].get('crawl_online'),
                'crawl_online_time': resp_json['crawlStatus'].get('crawl_online_time'),
                'query': resp_json['crawlSeed'],
            }))
            self.log.info('发送企业名称到工商调度消息队列: comapny = {company}'.format(company=company_name.encode('utf-8')))
            return self.to_string(download_rsp)
        except Exception as err:
            self.log.error("process failed, err[%s]" % (repr(err)))
            self.log.exception(err)

            download_rsp = DownLoadRsp(status=CrawlStatus.CRAWL_FAILT, )
            return self.to_string(download_rsp)
            # return download_rsp

    def do_output(self, body):
        return True
