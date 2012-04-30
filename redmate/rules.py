import keyformat

class Db2RedisRule(object):
    def __init__(self, *args, **kwargs):
        self.table = kwargs.get("table")
        self.query = kwargs.get("query")
        self.params = kwargs.get("params")
        self.key_pattern = keyformat.KeyPattern(kwargs.get("key_pattern"))
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
            key = self.key_pattern.format(row, rows)
            self._with_pipeline(key, row, rows, pipeline)
        pipeline.execute()

    def _query(self, db):
        return db.select(query=self.query, params=self.params)

    def _with_pipeline(key, row, pipeline):
        raise NotImplementedError

class ToHashRule(Db2RedisRule):

    def __init__(self, *args, **kwargs):
        super(ToHashRule, self).__init__(*args, **kwargs)

    def _with_pipeline(self, key, row, rows, pipeline):
        pipeline.hmset(key, rows.make_dict(row))


class ToListRule(Db2RedisRule):

    def __init__(self, *args, **kwargs):
        super(ToListRule, self).__init__(*args, **kwargs)
        self.transform = kwargs.get("transform", lambda r: r[0])

    def _with_pipeline(self, key, row, rows, pipeline):
        pipeline.lpush(key, self.transform(row))
        
class ToSetRule(Db2RedisRule):

    def __init__(self, *args, **kwargs):
        super(ToSetRule, self).__init__(*args, **kwargs)
        self.transform = kwargs.get("transform", lambda r: r[0])

    def _with_pipeline(self, key, row, rows, pipeline):
        pipeline.sadd(key, self.transform(row))

class ToSortedSetRule(ToSetRule):
    def __init__(self, *args, **kwargs):
        super(ToSortedSetRule, self).__init__(*args, **kwargs)
        self.score = kwargs.get("score")
        if not self.score:
            raise ValueError("No score function or value specified")

    def _with_pipeline(self, key, row, rows, pipeline):
        pipeline.zadd(key, self._get_score(row, rows), self.transform(row))

    def _get_score(self, row, rows):
        if callable(self.score):
            return self.score(row)
        elif type(self.score) == type(""):
            colnum = rows.columns.index(self.score)
            return row[colnum]
        elif self._score_is_number():
            return self.score

    def _score_is_number(self):
        score_type = type(self.score)
        return score_type in [type(1), type(1L), type(0.1)]
