"""Suppliers module.
"""
from . import utils
from . import messaging

class DbSupplier(object):

    def __init__(self, query, params=None):
        self.query = query
        self.params = params

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.cursor.close()

    def __call__(self, conn):
        self.cursor = conn.cursor()
        if self.params:
            self.cursor.execute(self.query, self.params)
        else:
            self.cursor.execute(self.query)
        self.columns = [d[0] for d in self.cursor.description]
        return self

    def __iter__(self):
        return self

    def next(self):
        result = self.cursor.fetchone()
        if not result:
            raise StopIteration
        return messaging.Message(self.columns, result)

    def __next__(self):
        return self.next()

    def __str__(self):
        return 'db(query={0})'.format(utils.truncate(self.query, 30))

class RedisSupplier(object):

    def __init__(self, keys_pattern, columns):
        self.keys_pattern = keys_pattern
        self.columns = columns

    def __enter__(self):
        self.keys = self.redis_reader.keys(self.keys_pattern)
        return self

    def __exit__(self, *args):
        self.keys = None
        self.key_set_it = None

    def __call__(self, redis_reader):
        self.redis_reader = redis_reader
        return self

    def __iter__(self):
        self.key_set_it = iter(self.keys)
        return self

    def next(self):
        next_key = next(self.key_set_it)
        entry = self._read(next_key)
        return messaging.Message(self.columns, entry)

    def __next__(self):
        return self.next()

    def make_dict(self, row):
        return dict(zip(self.columns, row))

    def _read(self, next_key):
        raise NotImplementedError

    def __str__(self):
        return "redis(key_pattern=%s)" % self.keys_pattern


class RedisHashSupplier(RedisSupplier):

    def _read(self, next_key):
        return self.redis_reader.read_hash(next_key, self.columns)

