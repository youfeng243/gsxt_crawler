#!/usr/bin/env python
# -*- coding=utf-8 -*-

import Queue
import traceback
from pykafka.client import KafkaClient


class Producer(object):
    def __init__(self, host, log):
        self.log = log
        try:
            self.client = KafkaClient(hosts=host)
        except Exception, e:
            self.log.error("producer connect kafka failed:%s\n%s" % (e.message, traceback.format_exc()))
            exit(1)


    def sync_produce(self, topic, message):
        try:
            topic = self.client.topics[topic]
            with topic.get_sync_producer() as producer:
                producer.produce(message)
        except Exception, e:
            self.log.error("sync produce fail:%s\n%s" % (e.message, traceback.format_exc()))
            exit(1)

    def async_produce(self, topic, message):
        try:
            with topic.get_producer(delivery_reports=True) as producer:
                count = 0
                while True:
                    count += 1
                    producer.produce(message, partition_key='{}'.format(count))
                    if count % 10 ** 5 == 0:  # adjust this or bring lots of RAM ;)
                        while True:
                            try:
                                msg, exc = producer.get_delivery_report(block=False)
                                if exc is not None:
                                    self.log.error('Failed to deliver msg {}: {}'.format(msg.partition_key, repr(exc)))
                                else:
                                    self.log.error('Successfully delivered msg {}'.format(msg.partition_key))
                            except Queue.Empty:
                                break
        except Exception, e:
            self.log.error("async produce fail:%s\n%s" % (e.message, traceback.format_exc()))
            exit(1)


class Consumer(object):
    def __init__(self, host, log):
        self.log = log
        try:
            self.client = KafkaClient(hosts=host)
        except Exception, e:
            self.log.error("consumer connect kafka failed:%s\n%s" % (e.message, traceback.format_exc()))
            exit(1)

    def competitive_consume(self, topic, consumer_group, zk_host):
        try:
            topic = self.client.topics[topic]
            balanced_consumer = topic.get_balanced_consumer(
               consumer_group=consumer_group,
               auto_commit_enable=True,
               # reset_offset_on_start=True,
               zookeeper_connect=zk_host
            )
        except Exception, e:
            self.log.error("consumer competitive consume fail:%s\n%s" % (e.message, traceback.format_exc()))
            exit(1)
        # for message in balanced_consumer:
        #    if message is not None:
        #        print message.offset, message.value
        return balanced_consumer

    def equal_consume(self, topic):
        try:
            topic = self.client.topics[topic]
            consumer = topic.get_simple_consumer()
        except Exception, e:
            self.log.error("equal consume failed:%s\n%s" % (e.message, traceback.format_exc()))
            exit(1)
        # for message in consumer:
        #     if message is not None:
        #         print message.offset, message.value
        return consumer

