import unittest
from mock import Mock
from redmate.mapping import Redis2DbMapper

class Redis2DbMapperTest(unittest.TestCase):

    def setUp(self):
        self.db = Mock(name="db-connection-mock")
        self.redis = Mock(name="redis-client-mock")
        self.mapper = Redis2DbMapper(self.redis, self.db)

    def test_hash_to_db(self):
        key_pattern = "key:*"
        columns = ("id", "name")
        keys = ("key:1", "key:2")
        vals = (("1", "x"), ("2", "y"))
        query = "update table set x = ?, y = ?"
        self.redis.keys.return_value = keys
        self.redis.hmget.side_effect = vals

        self.mapper.map_hash(key_pattern, columns).to(query)
        self.mapper.run()

        self.redis.keys.assert_called_with(key_pattern)
        self.redis.hmget.assert_any_call(keys[0], columns)
        self.redis.hmget.assert_any_call(keys[1], columns)
        self.db.cursor.assert_called_with()
        self.db.cursor().execute.assert_any_call(query, vals[0])
        self.db.cursor().execute.assert_any_call(query, vals[1])
        self.db.cursor().close.assert_called_with()

if __name__ == "__main__":
    unittest.main()
