class Db2RedisRule(object):
    def __init__(self, *args, **kwargs):
        self.table = kwargs.get("table")
        self.query = kwargs.get("query")
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
        rows = db.select(query=self.query, as_hash=True)
        if not rows:
            return
        key_pattern = self.key_pattern
        pipe = redis.pipeline()
        for row in rows:
            key = key_pattern.format(**row)
            pipe.hmset(key, row)
        pipe.execute()

    
