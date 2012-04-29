class RowIterator:

    def __init__(self, cursor, as_hash):
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

class Adapter:
    """
    Simple adapter for a database connection.
    """
    def __init__(self, connection):
        self.conn = connection

    def select(self, query, params=None, as_hash=False):
        """
        Executes search query and returns rows iterator.
        """
        cursor = self.conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        return RowIterator(cursor, as_hash)
        
