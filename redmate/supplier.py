import reader

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
        return result

    def make_dict(self, row):
        return dict(zip(self.columns, row))

    def __next__(self):
        return self.next()

