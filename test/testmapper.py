import unittest
from mock import Mock
import redmate

class MapperTest(unittest.TestCase):

    def setUp(self):
        self.db = Mock(name="db-adapter-mock")
        self.redis = Mock(name="redis-client-mock")
        self.mapper = redmate.Mapper(self.db, self.redis)

    def test_map_single_row_from_to_hash(self):
        """
        to_hash rule should map table with single row
        to a hash with a single key.
        """
        row = {"id": 1, "field": "value"}

        self.db.select.return_value = [row]


        self.mapper.to_hash(table="MyTable", key_pattern="key")
        self.mapper.run()

        self.db.select.assert_called_with("select * from MyTable", as_hash=True)
        self.redis.pipeline.assert_called_with()
        self.redis.pipeline().hmset.assert_called_with("key", row)
        self.redis.pipeline().execute.assert_called_with()

if __name__ == "__main__":
    unittest.main()
