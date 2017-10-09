# -*- coding:utf-8 -*-

import pymongo
import traceback


class MongoDB(object):
    def __init__(self, host, port, database, collection):
        try:
            self.conn = pymongo.MongoClient(host, port)
            db = self.conn['admin']
            db.authenticate('readall', 'readall')
            self.db = self.conn['%s' % database]
            self.collection = self.db['%s' % collection]
        except Exception, e:
            print "connect mongob fail:%s" % traceback.format_exc()
            exit(1)

    def find(self, site_url):
        try:
            return self.collection.find({'metadata.site': site_url})
        except Exception, e:
            print "find mongodb fail:%s" % traceback.format_exc()
            exit(1)

    def find_site_url(self):
        try:
            # return self.collection.find({}, {'metadata.site': 1, '_id': 0})
            return self.collection.distinct('metadata.site')
        except Exception, e:
            print "find site url fail:%s" % traceback.format_exc()
            exit(1)


class StatisticSingleSite(object):
    def __init__(self, host, port, database, collection):
        self.extract_success = {}
        self.extract_fail = {}
        self.extract_skip = {}
        self.extract_total = {}
        self.download_fail = {}
        self.db = MongoDB(host, port, database, collection)

    def get_metadata_by_site_url(self, site_url):
        return self.db.find(site_url)

    def parse_site_extract(self, site_url):
        metadata = self.get_metadata_by_site_url(site_url)
        self.extract_fail[site_url] = 0
        self.extract_success[site_url] = 0
        self.extract_skip[site_url] = 0
        self.extract_total[site_url] = 0
        self.download_fail[site_url] = 0

        for record in metadata:
            self.extract_success[site_url] += record['extract_success']['daily'] if 'extract_success' in record else 0
            self.extract_fail[site_url] += record['extract_fail']['daily'] if 'extract_fail' in record else 0
            self.extract_skip[site_url] += record['extract_skip']['daily'] if 'extract_skip' in record else 0
            self.download_fail[site_url] += record['download_fail']['daily'] if 'download_fail' in record else 0

        # self.extract_total[site_rul] = (record['extract_success']['daily'] or 0 +
        #                                 record['extract_fail']['daily'] or 0 +
        #                                 record['extract_skip']['daily'] or 0)

        self.extract_total[site_url] = (self.extract_skip[site_url] + self.extract_success[site_url] +
                                        self.extract_fail[site_url] + self.download_fail[site_url])

        return {'success': self.extract_success[site_url], 'fail': self.extract_fail[site_url],
                'skip': self.extract_skip[site_url], 'total': self.extract_total[site_url],
                'download_fail': self.download_fail[site_url]}


class StatisticSiteFailRate(object):
    def __init__(self):
        self.all_sites_extract_success = 0
        self.all_sites_extract_fail = 0
        self.all_sites_download_fail = 0
        self.all_sites_extract_total = 0

    def statistic_all_site(self, host, port, database, collection):
        statistic_single_site = StatisticSingleSite(host, port, database, collection)
        site_urls = statistic_single_site.db.find_site_url()
        for site_url in site_urls:
            statistic = statistic_single_site.parse_site_extract(site_url)
            print '%s: %s' % (site_url, statistic)
            self.all_sites_extract_fail += statistic['fail']
            self.all_sites_download_fail += statistic['download_fail']
            self.all_sites_extract_total += statistic['total']

        print "download_fail_rate:%.3f" % (self.all_sites_download_fail/self.all_sites_extract_total)
        print "extractor_fail_rate:%.3f" % (self.all_sites_extract_fail/self.all_sites_extract_total)


def main():
    host = '172.16.215.15'
    port = 40042
    database = 'task_collect'
    collection = 'extract_task'

    statistic_site_fail_rate = StatisticSiteFailRate()
    statistic_site_fail_rate.statistic_all_site(host, port, database, collection)

if __name__ == '__main__':
    main()
