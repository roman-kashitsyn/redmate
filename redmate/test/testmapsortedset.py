import unittest
from mock import Mock
import redmate

class MapToSortedSetTest(unittest.TestCase):

    def setUp(self):
        self.db = Mock(name="db-adapter-mock")
        self.redis = Mock(name="redis-client-mock")
        self.mapper = redmate.Mapper(self.db, self.redis)
        self.query = "select id, name, salary from table where salary > %d"
        self.params = (50,)
        self.cols = ("id", "name", "salary")
        self.key = "emp:by:salary"
        self.rows = ((1, "M", 200), (2, "S", 100), (3, "R", 230))
        self.db.select.return_value = redmate.test.mock_row_iterator(
            self.rows, self.cols)
    
    def test_map_query_to_sorted_set(self):
        """
        to_sorted_set should map rows with ZADD command using
        given score function
        """
        key = self.key
        self.mapper.to_sorted_set(query=self.query, key_pattern=key,
                                  params=self.params, score=lambda r: r[2])
        self.mapper.run()

        self.db.select.assert_called_with(query=self.query, params=(50,))
        self.redis.pipeline.assert_called_with()
        self.redis.pipeline().zadd.assert_any_call(key, self.rows[0][2], self.rows[0][0])
        self.redis.pipeline().zadd.assert_any_call(key, self.rows[1][2], self.rows[1][0])
        self.redis.pipeline().zadd.assert_any_call(key, self.rows[2][2], self.rows[2][0])
        self.redis.pipeline().execute.assert_called_with()

    def test_map_to_sorted_set_with_fixed_numeric_score(self):
        """
        to_sorted_set with fixed score should map rows with ZADD
        command using constant score
        """
        key = self.key
        self.mapper.to_sorted_set(query=self.query, key_pattern=key,
                                  params=self.params, score=5)
        self.mapper.run()

        self.redis.pipeline.assert_called_with()
        self.redis.pipeline().zadd.assert_any_call(key, 5, self.rows[0][0])
        self.redis.pipeline().zadd.assert_any_call(key, 5, self.rows[1][0])
        self.redis.pipeline().zadd.assert_any_call(key, 5, self.rows[2][0])
        self.redis.pipeline().execute.assert_called_with()

    def test_map_fails_without_score(self):
        self.assertRaises(ValueError, self.mapper.to_sorted_set,
                          key_pattern="set", table="table")
