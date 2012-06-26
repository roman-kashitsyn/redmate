"""
Mapping rules module.
"""
import logging

from . import writer
from . import reader
from . import consumer
from . import supplier

log = logging.getLogger('redmate.mapping')

class MappingRule(object):

    def __init__(self, supplier, consumer):
        self.supplier = supplier
        self.consumer = consumer

    def __call__(self, reader, writer):
        consumer = self.consumer
        processed = 0
        with self.supplier(reader) as supplier:
            for entry in supplier:
                consumer(writer, supplier, entry)
                processed += 1
        return processed

    def __str__(self):
        return '{{ {0} -> {1} }}'.format(self.supplier, self.consumer)

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

    def map(self, *args, **kwargs):
        self.__supplier = supplier.DbSupplier(*args, **kwargs)
        return self

    def to_string(self, *args, **kwargs):
        self.__make_rule(consumer.RedisStringConsumer(*args, **kwargs))

    def to_hash(self, *args, **kwargs):
        self.__make_rule(consumer.RedisHashConsumer(*args, **kwargs))

    def to_set(self, *args, **kwargs):
        self.__make_rule(consumer.RedisSetConsumer(*args, **kwargs))

    def to_list(self, *args, **kwargs):
        self.__make_rule(consumer.RedisListConsumer(*args, **kwargs))

    def to_sorted_set(self, *args, **kwargs):
        self.__make_rule(consumer.RedisSortedSetConsumer(*args, **kwargs))

    def __make_rule(self, consumer):
        self.add_rule(MappingRule(self.__supplier, consumer))

class Redis2DbMapper(Mapper):
    def __init__(self, redis, connection):
        redis_reader = reader.RedisReader(redis)
        db_writer = writer.DbWriter(connection)
        super(Redis2DbMapper, self).__init__(redis_reader, db_writer)

    def map_hash(self, *args, **kwargs):
        self.__supplier = supplier.RedisHashSupplier(*args, **kwargs)
        return self

    def map_set(self, *args, **kwargs):
        self.__supplier = supplier.RedisSetSupplier(*args, **kwargs)

    def map_sorted_set(self, *args, **kwargs):
        self.__supplier = supplier.RedisSortedSetSupplier(*args, **kwargs)

    def to(self, *args, **kwargs):
        cons = consumer.DbConsumer(*args, **kwargs)
        self.add_rule(MappingRule(self.__supplier, cons))

