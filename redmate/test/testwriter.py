import unittest
from mock import Mock
from redmate.writer import RedisWriter

class RedisWriterTest(unittest.TestCase):

    def setUp(self):
        self.redis= Mock(name="redis")

    def test_write_string(self):
        with RedisWriter(self.redis) as r:
            r.set("key", "value")

        self.redis.pipeline.assert_called_with()
        self.redis.pipeline().set.assert_called_with("key", "value")
        self.redis.pipeline().execute.assert_called_with()

    def test_put_to_hash(self):
        with RedisWriter(self.redis) as r:
            r.put_to_hash("key", "h", "v")

        self.redis.pipeline.assert_called_with()
        self.redis.pipeline().hput("key", "h", "v")
        self.redis.pipeline().execute.assert_called_with()

    def test_add_to_list(self):
        with RedisWriter(self.redis) as r:
            r.add_to_list("key", "val")

        self.redis.pipeline.assert_called_with()
        self.redis.pipeline().lpush("key", "val")
        self.redis.pipeline().execute.assert_called_with()

    def test_add_to_set(self):
        with RedisWriter(self.redis) as r:
            r.add_to_set("key", "elem")
        self.redis.pipeline.assert_called_with()
        self.redis.pipeline().sadd("key", "elem")
        self.redis.pipeline().execute.assert_called_with()

    def test_add_to_sorted_set(self):
        with RedisWriter(self.redis) as r:
            r.add_to_sorted_set("key", 20, "elem")
        self.redis.pipeline.assert_called_with()
        self.redis.pipeline().zadd("key", 20, "elem")
        self.redis.pipeline().execute.assert_called_with()

    def test_pipeline_support(self):
        limit = 11
        with RedisWriter(self.redis, max_pipelined=5) as r:
            for x in range(limit):
                r.add_to_set("key", x)

        for x in range(limit):
            self.redis.pipeline().sadd.assert_any_call("key", x)
        self.assertEqual(3, self.redis.pipeline().execute.call_count)

if __name__ == "__main__":
    unittest.main()
