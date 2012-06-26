"""
Definitions of readers.
"""

class RowIterator(object):
    """
    Iterator for DB result set rows.
    """
    def __init__(self, cursor):
        self.cursor = cursor
        self.columns = [d[0] for d in self.cursor.description]

    def __iter__(self):
        return self

    def next(self):
        result = self.cursor.fetchone()
        if not result:
            raise StopIteration
        return result

    def make_dict(self, row):
        return dict(zip(self.columns, row))

    def __next__(self):
        return self.next()

class DbReader(object):
    """
    Simple adapter for a database connection.
    """
    def __init__(self, connection):
        """
        Initializes reader with given database connection.

        connection -- connection compatible with PEP 249
        """
        self.conn = connection

    def __call__(self, query, params=None):
        """
        Executes search query and returns row iterator.

        query -- query to select data from
        """
        cursor = self.conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        return RowIterator(cursor)

class RedisReader(object):
    """
    Reader for reader data structures.
    """
    def __init__(self, redis):
        self.redis = redis

    def keys(self, pattern):
        return self._command("keys", pattern)

    def read_hash(self, key, columns):
        return self._command("hmget", key, columns)

    def read_set(self, key):
        return self._command("smembers", key)

    def _command(self, name, *args, **kwargs):
        cmd = getattr(self.redis, name)
        return cmd(*args, **kwargs)
