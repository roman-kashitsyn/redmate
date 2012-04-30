import unittest
from mock import Mock
import redmate

class MapToListTest(unittest.TestCase):
    
    def setUp(self):
        self.db = Mock(name="db-adapter-mock")
        self.redis = Mock(name="redis-client-mock")
        self.mapper = redmate.Mapper(self.db, self.redis)
        self.db.select.return_value = redmate.test.mock_row_iterator(
            ((1, "Spam"), (2, "Egg")), "id, word")

    def test_map_query_to_list(self):
        """
        to_list should map rows to list taking first column as
        value by default
        """
        query = "select * from [Table]"
        self.mapper.to_list(query=query, key_pattern="list")
        self.mapper.run()
        
        self.db.select.assert_called_with(query=query, params=None)
        self.redis.pipeline.assert_called_with()
        self.redis.pipeline().lpush.assert_any_call("list", 1)
        self.redis.pipeline().lpush.assert_any_call("list", 2)
        self.redis.pipeline().execute.assert_called_with()
        

    def test_map_query_to_list_with_transform(self):
        """
        to_list should map table rows to list with respect to
        given transform
        """
        join_columns = lambda lst: ' '.join(map(str, lst))
        query = "select * from [Table]"
        self.mapper.to_list(query=query, key_pattern="list",
                            transform=join_columns)
        self.mapper.run()
        
        self.db.select.assert_called_with(query=query, params=None)
        self.redis.pipeline.assert_called_with()
        self.redis.pipeline().lpush.assert_any_call("list", "1 Spam")
        self.redis.pipeline().lpush.assert_any_call("list", "2 Egg")
        self.redis.pipeline().execute.assert_called_with()
