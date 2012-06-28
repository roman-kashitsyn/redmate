"""
Mapping rules module.
"""
import logging

from . import writer
from . import reader
from . import consumer
from . import supplier
from . import messaging

log = logging.getLogger('redmate.mapping')

class MappingRule(object):

    def __init__(self, supplier, channel):
        self.supplier = supplier
        self.channel = channel

    def __call__(self, reader, writer):
        channel = self.channel
        processed = 0
        with self.supplier(reader) as supplier:
            for message in supplier:
                channel.publish(message)
                processed += 1
        return processed

    def __str__(self):
        return '{{ {0} -> {1} }}'.format(self.supplier, self.channel)

class Mapper(object):

    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self._rules = []

    def add_rule(self, rule):
        self._rules.append(rule)

    def run(self):
        total = 0
        with self.writer as writer:
            for rule in self._rules:
                log.info('Running rule %s', rule)
                processed = rule(self.reader, writer)
                log.info('Processed %d records', processed)
                total += processed
        return total

class Db2RedisMapper(Mapper):

    def __init__(self, connection, redis):
        super(Db2RedisMapper, self).__init__(connection, writer.RedisWriter(redis))
        self._channels = set()

    def map(self, *args, **kwargs):
        self.__supplier = supplier.DbSupplier(*args, **kwargs)
        self.__channel = messaging.Channel()
        return self

    def to_string(self, *args, **kwargs):
        self.__make_rule(consumer.RedisStringConsumer(self.writer, *args, **kwargs))
        return self

    def to_hash(self, *args, **kwargs):
        self.__make_rule(consumer.RedisHashConsumer(self.writer, *args, **kwargs))
        return self

    def to_set(self, *args, **kwargs):
        self.__make_rule(consumer.RedisSetConsumer(self.writer, *args, **kwargs))
        return self

    def to_list(self, *args, **kwargs):
        self.__make_rule(consumer.RedisListConsumer(self.writer, *args, **kwargs))
        return self

    def to_sorted_set(self, *args, **kwargs):
        self.__make_rule(consumer.RedisSortedSetConsumer(self.writer, *args, **kwargs))
        return self

    def __make_rule(self, consumer):
        channel = self.__channel
        channel.add_subscriber(consumer)
        if not channel.name in self._channels:
            self.add_rule(MappingRule(self.__supplier, channel))
            self._channels.add(channel.name)

class Redis2DbMapper(Mapper):
    def __init__(self, redis, connection):
        redis_reader = reader.RedisReader(redis)
        db_writer = writer.DbWriter(connection)
        super(Redis2DbMapper, self).__init__(redis_reader, db_writer)

    def map_hash(self, *args, **kwargs):
        self.__supplier = supplier.RedisHashSupplier(*args, **kwargs)
        self.__channel = messaging.Channel()
        return self

    def to(self, *args, **kwargs):
        cons = consumer.DbConsumer(self.writer, *args, **kwargs)
        self.__channel >> cons
        self.add_rule(MappingRule(self.__supplier, self.__channel))

