import keyformat

def take_first(row):
    return row[0]

def truncate(line, max_length=100, suffix="."):
    if len(line) > max_length:
        return line[:max_length - 3] + suffix * 3
    return line

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

    def run(self, db, redis, max_pipelined=None):
        rows = self._query(db)
        if not rows:
            return
        
        for row in rows:
            key = self.key_pattern.format(row, rows)
            self._with_pipeline(key, row, rows, redis)

    def _name(self):
        raise NotImplementedError

    def _query(self, db):
        return db.select(query=self.query, params=self.params)

    def _with_pipeline(key, row, pipeline):
        raise NotImplementedError

    def __str__(self):
        return "{0}(query=<{1}>, key_pattern=<{2}>)"\
            .format(self._name(), truncate(self.query, max_length=30), self.key_pattern)

class ToHashRule(Db2RedisRule):

    def __init__(self, *args, **kwargs):
        super(ToHashRule, self).__init__(*args, **kwargs)

    def _with_pipeline(self, key, row, rows, pipeline):
        pipeline.hmset(key, rows.make_dict(row))

    def _name(self):
        return "to_hash"

class ToListRule(Db2RedisRule):

    def __init__(self, *args, **kwargs):
        super(ToListRule, self).__init__(*args, **kwargs)
        self.transform = kwargs.get("transform", take_first)

    def _with_pipeline(self, key, row, rows, pipeline):
        pipeline.lpush(key, self.transform(row))

    def _name(self):
        return "to_list"
        
class ToSetRule(Db2RedisRule):

    def __init__(self, *args, **kwargs):
        super(ToSetRule, self).__init__(*args, **kwargs)
        self.transform = kwargs.get("transform", take_first)

    def _with_pipeline(self, key, row, rows, pipeline):
        pipeline.sadd(key, self.transform(row))

    def _name(self):
        return "to_set"

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

    def _name(self):
        return "to_sorted_set"

    def __str__(self):
        return "{0}(query=<{1}>, key_pattern=<{2}>, score=<{3}>)" \
            .format(self._name(), truncate(self.query, max_length=30), \
                        self.key_pattern, self.score)
