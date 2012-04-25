import unittest
from mock import Mock
import redmate

class MapperTest(unittest.TestCase):

    def setUp(self):
        self.db = Mock(name="db-adapter-mock")
        self.redis = Mock(name="redis-client-mock")
        self.mapper = redmate.Mapper(self.db, self.redis)

    def test_map_table_to_hash(self):
        """
        to_hash should map table rows to a hash
        using spedified pattern for keys
        """
        rows = ({"id": 1, "field": "abc"}, {"id": 2, "field": "bcd"})

        self.db.select.return_value = rows


        self.mapper.to_hash(table="[MyTable]", key_pattern="row:{id}")
        self.mapper.run()

        query="select * from [MyTable]"
        self.db.select.assert_called_with(query=query, params=None, as_hash=True)
        self.redis.pipeline.assert_called_with()
        self.redis.pipeline().hmset.assert_any_call("row:1", rows[0])
        self.redis.pipeline().hmset.assert_any_call("row:2", rows[1])
        self.redis.pipeline().execute.assert_called_with()

    def test_map_query_to_hash(self):
        """
        to_hash should map query to a hash using specified
        pattern for keys
        """
        rows = ({"q": 1, "x": 0}, {"q": 2, "x": 1})
        self.db.select.return_value = rows
        
        query = "select q, x from T where x < 2"
        self.mapper.to_hash(query=query, key_pattern="x:{x}")
        self.mapper.run()
        
        self.db.select.assert_called_with(query=query, params=None, as_hash=True)
        self.redis.pipeline().hmset.assert_any_call("x:0", rows[0])
        self.redis.pipeline().hmset.assert_any_call("x:1", rows[1])

    def test_map_query_with_params_to_hash(self):
        """
        to_hash should map query with params
        """
        rows = ({"q": 1, "x": 0}, {"q": 2, "x": 1})
        self.db.select.return_value = rows
        
        query = "select q, x from T where x < ?"
        self.mapper.to_hash(query=query, params=(2), key_pattern="x:{x}")
        self.mapper.run()
        
        self.db.select.assert_called_with(
            query=query, params=(2), as_hash=True)
        self.redis.pipeline().hmset.assert_any_call("x:0", rows[0])
        self.redis.pipeline().hmset.assert_any_call("x:1", rows[1])

    def test_to_hash_errors(self):
        """
        to_hash method should raise a ValueError if
        request is not all parameters specified
        """
        self.assertRaises(ValueError, self.mapper.to_hash)
        self.assertRaises(ValueError, self.mapper.to_hash, table="", key_pattern="")
        self.assertRaises(ValueError, self.mapper.to_hash, table="A", key_pattern="")
        self.assertRaises(ValueError, self.mapper.to_hash, query="A")

if __name__ == "__main__":
    unittest.main()
