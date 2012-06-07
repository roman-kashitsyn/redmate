import db
import rules
import logging

log = logging.getLogger('redmate')

def Db(connection):
    return db.Adapter(connection)


class Mapper(object):

    def __init__(self, db, redis):
        self.db = db
        self.redis = redis
        self._rules = []
        self.max_pipelined = self._default_max_pipelined()

    def to_hash(self, *args, **kwargs):
        """
        Adds rule to map table or query rows into hash.
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
        Adds rule to map table or query into set.
        """
        self._rules.append(rules.ToSetRule(*args, **kwargs))

    def to_sorted_set(self, *args, **kwargs):
        """
        Adds rule to map table or query into sorted set.
        """
        self._rules.append(rules.ToSortedSetRule(*args, **kwargs))

    def _default_max_pipelined(self):
        """
        Returns default number of commands pipelined with redis client
        """
        return 10

    def run(self):
        """
        Runs the mapping process.
        """
        db, redis = self.db, self.redis
        log.info("Got %d rules to run", len(self._rules))
        for rule in self._rules:
            log.info("Running rule: %s", rule)
            num_records = rule.run(db, redis, max_pipelined=self.max_pipelined)
            log.info("Records mapped: %s", num_records)
        log.info("Done")
