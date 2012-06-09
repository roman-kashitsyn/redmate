"""
Definitions of writers.
"""

class RedisWriter(object):
    """
    Writer that use redis as data storage.
    The reason why not just use redis is automatic
    pipelining and descriptive method names.
    Class implements contex manager protocol for convenience
    pipelining.
    """
    def __init__(self, redis, pipelined=True, max_pipelined=10):
        """
        Initializes writer with redis connection.

        redis -- redis client intance
        pipelined -- use pipelines (True by default)
        max_pipelined -- maximum number of commands to pipeline
        """
        self.redis = redis
        self.pipelined = pipelined
        self.max_pipelined = max_pipelined

    def __enter__(self):
        if self.pipelined:
            self.executor = self.redis.pipeline()
            self.cmd_num = 0
        else:
            self.executor = self.redis
        return self

    def __exit__(self, *args):
        # Execute commands left in the pipeline
        if self.pipelined and self.cmd_num:
            self.executor.execute()
            self.cmd_num = 0

    def set(self, key, value):
        """
        Puts given string to redis.

        key -- redis key
        value -- string to put
        """
        self._execute("set", key, value)

    def add_to_set(self, key, value):
        """
        Adds given value to set with given key.

        key -- key of the redis set
        value -- value to put
        """
        self._execute("sadd", key, value)

    def add_to_sorted_set(self, key, score, value):
        """
        Adds given value to sorted set using given key and score.

        key -- key of the redis sorted set
        score -- score to put value with
        value -- value to put
        """
        self._execute("zadd", key, score, value)

    def add_to_list(self, key, value):
        """
        Adds given value to redis list.

        key -- key of the redis key
        value -- value to put
        """
        self._execute("lpush", key, value)

    def put_to_hash(self, key, hkey, value):
        """
        Puts given value to key of the hash.

        key -- key of the redis hash
        hkey -- key in the hash
        value -- value to put
        """
        self._execute("hset", key, hkey, value)

    def _execute(self, cmd, *args):
        command = getattr(self.executor, cmd)
        if self.pipelined:
            if self.cmd_num >= self.max_pipelined:
                self.executor.execute()
                self.executor = self.redis.pipeline()
                self.cmd_num = 0
            self.cmd_num += 1
        command(*args)

