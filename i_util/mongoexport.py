#!/usr/bin/env python
# -*- coding=utf-8 -*-

import pymongo
import argparse
import logging
import time


class MongoDB(object):
    def __init__(self, host, port, database, collection):
        try:
            self.conn = pymongo.MongoClient(host, port)
            self.database = self.conn['%s' % database]
            self.collection = self.database['%s' % collection]
        except Exception, e:
            print "connect mongodb fail:%s" % e.message
            exit(1)

    def insert(self, value_list):
        try:
            self.collection.insert(value_list)
            logging.info("insert data completely")
        except Exception, e:
            logging.error("insert data fail:%s" % e.message)


def argument_parse():
    parse = argparse.ArgumentParser(description='a mongodb export text data tool')
    parse.add_argument('-host', action="store", dest='host', type=str, required=True, help="mongodb host ip")
    parse.add_argument('-port', action="store", dest='port', type=int, required=True, help="mongodb port")
    parse.add_argument('-f', action="store", dest="file", type=file, required=True, help="text file name")
    parse.add_argument('-d', action="store", dest="database", type=str, required=True, help="database name")
    parse.add_argument('-c', action="store", dest="collection", type=str, required=True, help="collection name")
    parse.add_argument('--fields', action="store", dest="fields", type=str, required=True,
                       help="read fields from text file")
    parse.add_argument('--version', action="version",version="%(prog)s 1.0")
    return parse.parse_args()


def parse_text(fd, fields):
    values = []
    text_line_number = 0
    fields = fields.split(',')
    fields_len = len(fields)
    line = fd.readline()
    while line:
        record = {}
        text_line_number += 1
        fields_values = line.split('\t')
        fields_values_len = len(fields_values)
        if fields_len != fields_values_len:
            logging.warning("data format error value:%s, fields:%s" % (fields, fields_values))
            continue

        for index in xrange(fields_len):
            record[fields[index]] = fields_values[index]
            record['_utime'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            record['_in_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

        values.append(record)
        line = fd.readline().strip()

    return values, text_line_number


if __name__ == '__main__':
    result = argument_parse()
    fd = result.file
    values, line_number = parse_text(fd, result.fields)
    mongo_conn = MongoDB(result.host, result.port, result.database, result.collection)
    mongo_conn.insert(values)
    logging.info("insert into mongodb records:%s" % line_number)
