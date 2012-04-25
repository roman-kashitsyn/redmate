import unittest
from mock import Mock
import redmate

class MapToListTest(unittest.TestCase):
    
    def setUp(self):
        self.db = Mock(name="db-adapter-mock")
        self.redis = Mock(name="redis-client-mock")
        self.mapper = redmate.Mapper(self.db, self.redis)

    def test_map_query_to_list(self):
        """
        to_list should map table rows to list
        """
        rows = [(1, "Spam"), (2, "Egg")]
        self.db.select.return_value = rows
        
        joiner = lambda lst: ' '.join(map(str, lst))
        query = "select * from [Table]"
        self.mapper.to_list(query=query, key="key", transformer=joiner)
        self.mapper.run()
        
        self.db.select.assert_called_with(query=query, params=None)
        self.redis.pipeline.assert_called_with()
        self.redis.pipeline().lpush.assert_any_call("key", "1 Spam")
        self.redis.pipeline().lpush.assert_any_call("key", "2 Egg")
        self.redis.pipeline().execute.assert_called_with()
        
        

    
