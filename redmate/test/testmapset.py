import unittest
from mock import Mock
import redmate

class MapToSetTest(unittest.TestCase):

    def setUp(self):
        self.db = Mock(name="db-adapter-mock")
        self.redis = Mock(name="redis-client-mock")
        self.mapper = redmate.Mapper(self.db, self.redis)
        self.db.select.return_value = [(1, "Spam"), (2, "Egg")]

    def test_map_query_to_set(self):
        """
        to_set should map rows to set taking first column as value
        by default
        """
        query = "select id, word from mems limit 2"
        self.mapper.to_set(query=query, key="set")
        self.mapper.run()
        
        self.db.select.assert_called_with(query=query, params=None)
        self.redis.pipeline.assert_called_with()
        self.redis.pipeline().sadd.assert_any_call("set", 1)
        self.redis.pipeline().sadd.assert_any_call("set", 2)
        self.redis.pipeline().execute.assert_called_with()
