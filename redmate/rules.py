class Db2RedisRule(object):
    def __init__(self, *args, **kwargs):
        self.table = kwargs.get("table")
        self.query = kwargs.get("query")
        self.params = kwargs.get("params")
        if not self.table and not self.query:
            raise ValueError("No either table or query is specified")
        if not self.query:
            self.query = "select * from {0}".format(self.table)

    def run(self, db, redis):
        rows = self._query(db)
        if not rows:
            return
        pipeline = redis.pipeline()
        for row in rows:
            self._with_pipeline(row, pipeline)
        pipeline.execute()

    def _query(self, db):
        return db.select(query=self.query, params=self.params)

    def _with_pipeline(row, pipeline):
        raise NotImplementedError

class ToHashRule(Db2RedisRule):

    def __init__(self, *args, **kwargs):
        super(ToHashRule, self).__init__(*args, **kwargs)
        self.key_pattern = kwargs.get("key_pattern")
        if not self.key_pattern:
            raise ValueError("No key pattern specified")

    def _query(self, db):
        return db.select(query=self.query, params=self.params, as_hash=True)

    def _with_pipeline(self, row, pipeline):
        key = self.key_pattern.format(**row)
        pipeline.hmset(key, row)


class ToListRule(Db2RedisRule):

    def __init__(self, *args, **kwargs):
        super(ToListRule, self).__init__(*args, **kwargs)
        self.key = kwargs.get("key")
        self.transform = kwargs.get("transform", lambda r: r[0])
        if not self.key:
            raise ValueError("No list key specified")

    def _with_pipeline(self, row, pipeline):
        pipeline.lpush(self.key, self.transform(row))
        
class ToSetRule(Db2RedisRule):

    def __init__(self, *args, **kwargs):
        super(ToSetRule, self).__init__(*args, **kwargs)
        self.key = kwargs.get("key")
        self.transform = kwargs.get("transform", lambda r: r[0])
        if not self.key:
            raise ValueError("No list key specified")

    def _with_pipeline(self, row, pipeline):
        pipeline.sadd(self.key, self.transform(row))

        

