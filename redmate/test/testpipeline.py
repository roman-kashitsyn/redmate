import unittest
from mock import Mock
import redmate

class PipeliningTest(unittest.TestCase):

    def setUp(self):
        self.db = Mock(name="db-mock")
        self.redis = Mock(name="redis-mock")
        self.mapper = redmate.Mapper(self.db, self.redis)
        self.db.select.return_value = redmate.test.mock_row_iterator(
            ((1,), (2,), (3,), (4,), (5,)), ("id"))

    def test_pipeline_uses_fixed_amount_of_commands(self):
        """
        It should be possible to control number of commands queued
        by redis client
        """
        self.mapper.max_pipelined = 2
        self.mapper.to_set(query="query", key_pattern="pat")
        self.mapper.run()

        self.assertEqual(3, self.redis.pipeline().execute.call_count)
        self.redis.pipeline().reset.assert_called_with()

    def test_pipeline_will_be_reset_if_exception_occurres(self):
        """
        Pipeline should be reset even if exception occurres
        """
        self.redis.pipeline().sadd.side_effect = Exception("unexpected error")
        self.mapper.to_set(query="query", key_pattern="pat")
        self.assertRaises(Exception, self.mapper.run)

        self.redis.pipeline.assert_called_with()
        self.redis.pipeline().reset.assert_called_with()
        
