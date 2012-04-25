class Db2RedisRule(object):
    def __init__(self, *args, **kwargs):
        self.table = kwargs.get("table")
        self.query = kwargs.get("query")
        self.params = kwargs.get("params")
        if not self.table and not self.query:
            raise ValueError("No either table or query is specified")
        if not self.query:
            self.query = "select * from {0}".format(self.table)

class ToHashRule(Db2RedisRule):

    def __init__(self, *args, **kwargs):
        super(ToHashRule, self).__init__(*args, **kwargs)
        self.key_pattern = kwargs.get("key_pattern")
        if not self.key_pattern:
            raise ValueError("No key pattern specified")

    def run(self, db, redis):
        rows = db.select(query=self.query, params=self.params, as_hash=True)
        if not rows:
            return
        key_pattern = self.key_pattern
        pipe = redis.pipeline()
        for row in rows:
            key = key_pattern.format(**row)
            pipe.hmset(key, row)
        pipe.execute()

class ToListRule(Db2RedisRule):

    def __init__(self, *args, **kwargs):
        super(ToListRule, self).__init__(*args, **kwargs)
        self.key = kwargs.get("key")
        self.transformer = kwargs.get("transformer", str)
        if not self.key:
            raise ValueError("No list key specified")

    def run(self, db, redis):
        rows = db.select(query=self.query, params=self.params)
        if not rows:
            return
        key = self.key
        transform = self.transformer
        pipe = redis.pipeline()
        for row in rows:
            value = transform(row)
            pipe.lpush(key, value)
        pipe.execute()
        

        

