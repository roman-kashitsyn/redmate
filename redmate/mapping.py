"""
Mapping rules module.
"""
from writer import RedisWriter
from supplier import DbSupplier
import consumer

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
        return "Mapping {" + str(self.supplier) + " -> " + \
                str(self.consumer) + "}"

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
                total += rule(self.reader, writer)
        return total

class Db2RedisMapper(Mapper):

    def __init__(self, connection, redis):
        super(Db2RedisMapper, self).__init__(connection, RedisWriter(redis))

    def map(self, *args, **kwargs):
        self.__supplier = DbSupplier(*args, **kwargs)
        return self

    def to_string(self, *args, **kwargs):
        self._make_rule(self.__supplier, \
                consumer.RedisStringConsumer(*args, **kwargs))

    def to_hash(self, *args, **kwargs):
        self._make_rule(self.__supplier, \
                consumer.RedisHashConsumer(*args, **kwargs))

    def to_set(self, *args, **kwargs):
        self._make_rule(self.__supplier, \
                consumer.RedisSetConsumer(*args, **kwargs))

    def to_list(self, *args, **kwargs):
        self._make_rule(self.__supplier, \
                consumer.RedisListConsumer(*args, **kwargs))

    def to_sorted_set(self, *args, **kwargs):
        self._make_rule(self.__supplier, \
                consumer.RedisSortedSetConsumer(*args, **kwargs))

    def _make_rule(self, supplier, consumer):
        self.add_rule(MappingRule(supplier, consumer))

