"""
Definitions of readers.
"""

class RowIterator(object):
    """
    Iterator for DB result set rows.
    """
    def __init__(self, cursor):
        self.cursor = cursor
        self.as_hash = as_hash
        self.columns = [d[0] for d in self.cursor.description]

    def __iter__(self):
        return self

    def next(self):
        result = self.cursor.fetchone()
        if not result:
            raise StopIteration
        if self.as_hash:
            return self.make_dict(result)
        else:
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

    def select(self, query, params=None):
        """
        Executes search query and returns row iterator.

        query -- query to select data from
        """
        cursor = self.conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        return RowIterator(cursor, as_hash)

