import unittest
from mock import Mock
import redmate

class MapperTest(unittest.TestCase):

    def setUp(self):
        self.db = Mock(name="db-adapter-mock")
        self.redis = Mock(name="redis-client-mock")
        self.mapper = redmate.Mapper(self.db, self.redis)

    def test_map_rows_to_hash(self):
        """
        to_hash rule should map table rows to a hash
        using spedified pattern for keys
        """
        rows = ({"id": 1, "field": "abc"}, {"id": 2, "field": "bcd"})

        self.db.select.return_value = rows


        self.mapper.to_hash(table="MyTable", key_pattern="row:{id}")
        self.mapper.run()

        self.db.select.assert_called_with(
            query="select * from MyTable", as_hash=True)
        self.redis.pipeline.assert_called_with()
        self.redis.pipeline().hmset.assert_any_call("row:1", rows[0])
        self.redis.pipeline().hmset.assert_any_call("row:2", rows[1])
        self.redis.pipeline().execute.assert_called_with()

    def test_to_hash_errors(self):
        """
        to_hash method should raise a ValueError if
        request is not all parameters specified
        """
        self.assertRaises(ValueError, self.mapper.to_hash)
        self.assertRaises(ValueError, self.mapper.to_hash, table="")
        self.assertRaises(ValueError, self.mapper.to_hash, query="")

if __name__ == "__main__":
    unittest.main()
