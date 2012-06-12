import unittest
from mock import Mock
from redmate.reader import RedisReader

class RedisReaderTest(unittest.TestCase):

    def setUp(self):
        self.redis = Mock(name="redis-client-mock")
        self.reader = RedisReader(self.redis)

    def test_read_keys(self):
        pattern = "keys:pattern:*"
        matched = ("keys:pattern:1", "keys:pattern:2")
        self.redis.keys.return_value = matched

        result = self.reader.keys(pattern)

        self.redis.keys.assert_called_with(pattern)
        self.assertEqual(result, matched)

    def test_read_hash(self):
        key = "my:hash"
        columns = ("col1", "col2", "col3")
        values = ("val1", "val2", "val3")
        self.redis.hmget.return_value = values

        result = self.reader.read_hash(key, columns)

        self.redis.hmget.assert_called_with(key, columns)
        self.assertEqual(result, values)

if __name__ == "__main__":
    unittest.main()

