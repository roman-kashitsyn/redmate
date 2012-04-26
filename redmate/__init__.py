import db
import rules

def Db(connection):
    return db.Adapter(connection)


class Mapper(object):

    def __init__(self, db, redis):
        self.db = db
        self.redis = redis
        self._rules = []

    def to_hash(self, *args, **kwargs):
        """
        Adds rule to map table or query rows into hash
        using given specification.
        """
        self._rules.append(rules.ToHashRule(*args, **kwargs))

    def to_list(self, *args, **kwargs):
        """
        Adds rule to map table or query rows into list
        using given specification.
        """
        self._rules.append(rules.ToListRule(*args, **kwargs))

    def to_set(self, *args, **kwargs):
        """
        Adds rule to map table or query into set
        """
        self._rules.append(rules.ToSetRule(*args, **kwargs))

    def run(self):
        db, redis = self.db, self.redis
        for rule in self._rules:
            rule.run(db, redis)
