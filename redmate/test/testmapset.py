import unittest
from mock import Mock, MagicMock
import redmate

class MapToSetTest(unittest.TestCase):

    def setUp(self):
        self.db = Mock(name="db-adapter-mock")
        self.redis = Mock(name="redis-client-mock")
        self.mapper = redmate.Mapper(self.db, self.redis)
        
        rows = [(1, "Spam"), (2, "Egg")]
        as_dict =  map(lambda x: dict(zip(["id", "word"], x)), rows)
        rowmock = MagicMock(name="row-iterator-mock")
        rowmock.__iter__.return_value = rows
        rowmock.make_dict.side_effect = as_dict
        self.db.select.return_value = rowmock

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

    def test_map_to_set_with_key_pattern(self):
        """
        to_set should map rows to set with respect to given
        key pattern
        """
        query = "select id, word from mems"
        
        self.mapper.to_set(query=query, key_pattern="mem:{id}:{word}")
        self.mapper.run()
        
        self.redis.pipeline().sadd.assert_any_call("mem:1:Spam", 1)
        self.redis.pipeline().sadd.assert_any_call("mem:2:Egg", 2)
        
