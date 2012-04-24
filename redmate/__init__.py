import db
import rules

def Db(connection):
    return db.Adapter(connection)


class Mapper(object):

    def __init__(self, db, redis):
        self.db = db
        self.redis = redis
        self.rules = []

    def to_hash(self, *args, **kwargs):
        self.rules.append(rules.ToHashRule(*args, **kwargs))

    def run(self):
        db, redis = self.db, self.redis
        for rule in self.rules:
            rule.run(db, redis)
