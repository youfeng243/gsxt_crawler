# -*- coding=utf-8 -*-

import Queue
import traceback
import logging
from pykafka.client import KafkaClient

logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s', level=logging.ERROR
)


class Producer(object):
    def __init__(self, host, log):
        self.log = log
        try:
            self.client = KafkaClient(hosts=host)
            self.partition_max_index = 3
            self.partition_key = 0
        except Exception, e:
            self.log.error("producer connect kafka failed:%s\n%s" % (e.message, traceback.format_exc()))
            exit(1)

    def sync_produce(self, topic, message):
        try:
            topic = self.client.topics[str(topic)]
            with topic.get_sync_producer() as producer:
                producer.produce(message)
        except Exception, e:
            self.log.error("sync produce fail:%s\n%s" % (e.message, traceback.format_exc()))

    def async_produce(self, topic, message):
        if self.partition_key >= self.partition_max_index:
            self.partition_key = 0

        try:
            topic = self.client.topics[str(topic)]
            with topic.get_producer(use_rdkafka=True, delivery_reports=False) as producer:
                producer.produce(message, partition_key='{}'.format(self.partition_key))

            self.partition_key += 1

        except Exception, e:
            self.log.error("async produce fail:%s\n%s" % (e.message, traceback.format_exc()))


class Consumer(object):
    def __init__(self, kafka_host, zk_host, log):
        self.log = log
        try:
            self.client = KafkaClient(hosts=kafka_host, zookeeper_hosts=zk_host)
        except Exception, e:
            self.log.error("consumer connect kafka failed:%s\n%s" % (e.message, traceback.format_exc()))
            exit(1)
        # self.zk_host = zk_host

    def competitive_consume(self, topic, consumer_group):
        try:
            topic = self.client.topics[str(topic)]
            balanced_consumer = topic.get_balanced_consumer(
                consumer_group=str(consumer_group),
                managed=True,
                auto_commit_enable=True,
                use_rdkafka=True
                # reset_offset_on_start=True,
                # zookeeper_connect=self.zk_host
            )
        except Exception, e:
            self.log.error("consumer competitive consume fail:%s\n%s" % (e.message, traceback.format_exc()))
            return None

        return balanced_consumer

    def equal_consume(self, topic):
        try:
            topic = self.client.topics[str(topic)]
            consumer = topic.get_simple_consumer()
        except Exception, e:
            self.log.error("equal consume failed:%s\n%s" % (e.message, traceback.format_exc()))
            return None

        return consumer


