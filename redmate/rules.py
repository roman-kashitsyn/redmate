class Db2RedisRule(object):
    def __init__(self, *args, **kwargs):
        self.table = kwargs.get("table")
        self.query = kwargs.get("query")
        self.params = kwargs.get("params")
        self.key_pattern = kwargs.get("key_pattern")
        self.key = kwargs.get("key")
        if not self.key and not self.key_pattern:
            raise ValueError("No either key or key_pattern specified")
        if self.key and self.key_pattern:
            raise ValueError("Ambigious rule: both key and key_pattern provided")
        if not self.table and not self.query:
            raise ValueError("No either table or query is specified")
        if not self.query:
            self.query = "select * from {0}".format(self.table)

    def run(self, db, redis):
        rows = self._query(db)
        if not rows:
            return
        pipeline = redis.pipeline()
        if self.key:
            for row in rows:
                self._with_pipeline(self.key, row, pipeline)
        elif self.key_pattern:
            for row in rows:
                params = row if isinstance(row, dict) else rows.make_dict(row)
                key = self.key_pattern.format(**params)
                self._with_pipeline(key, row, pipeline)
                
        pipeline.execute()

    def _query(self, db):
        return db.select(query=self.query, params=self.params)

    def _with_pipeline(key, row, pipeline):
        raise NotImplementedError

class ToHashRule(Db2RedisRule):

    def __init__(self, *args, **kwargs):
        super(ToHashRule, self).__init__(*args, **kwargs)

    def _query(self, db):
        return db.select(query=self.query, params=self.params, as_hash=True)

    def _with_pipeline(self, key, row, pipeline):
        pipeline.hmset(key, row)


class ToListRule(Db2RedisRule):

    def __init__(self, *args, **kwargs):
        super(ToListRule, self).__init__(*args, **kwargs)
        self.transform = kwargs.get("transform", lambda r: r[0])

    def _with_pipeline(self, key, row, pipeline):
        pipeline.lpush(key, self.transform(row))
        
class ToSetRule(Db2RedisRule):

    def __init__(self, *args, **kwargs):
        super(ToSetRule, self).__init__(*args, **kwargs)
        self.transform = kwargs.get("transform", lambda r: r[0])

    def _with_pipeline(self, key, row, pipeline):
        pipeline.sadd(key, self.transform(row))

