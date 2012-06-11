import unittest
from mock import Mock
from redmate.test.mockdb import mock_db
from redmate.utils import take_nth
from redmate.mapping import Db2RedisMapper

class Db2RedisMapperTest(unittest.TestCase):

    def setUp(self):
        self.redis = Mock(name='redis-client-mock')
        self.db = mock_db(('id', 'name'), [(1, 'A'), (2, 'B'), None])
        self.mapper = Db2RedisMapper(self.db, self.redis)
        self.query = 'select id, name from Table'

    def test_map_to_string(self):
        self.mapper.map(query=self.query).to_string(key_pattern='row:{id}',
                transform = take_nth(1))
        self.mapper.run()

        self.common_asserts()
        self.redis.pipeline().set.assert_any_call('row:1', 'A')
        self.redis.pipeline().set.assert_any_call('row:2', 'B')

    def test_map_to_hash(self):
        self.mapper.map(query=self.query).to_hash(key_pattern='row:{id}')
        self.mapper.run()

        self.common_asserts()
        self.redis.pipeline().hmset.assert_any_call('row:1', {'id':1, 'name':'A'})
        self.redis.pipeline().hmset.assert_any_call('row:2', {'id':2, 'name':'B'})

    def test_map_to_set(self):
        self.mapper.map(query=self.query).to_set(key_pattern='set:{name}')
        self.mapper.run()

        self.common_asserts()
        self.redis.pipeline().sadd.assert_any_call('set:A', 1)
        self.redis.pipeline().sadd.assert_any_call('set:B', 2)

    def test_map_to_list(self):
        self.mapper.map(query=self.query).to_list(key_pattern='list:{0}', \
                transform=take_nth(1))
        self.mapper.run()

        self.common_asserts()
        self.redis.pipeline().lpush.assert_any_call('list:1', 'A')
        self.redis.pipeline().lpush.assert_any_call('list:2', 'B')

    def test_map_to_sorted_set_by_score_column_name(self):
        self.mapper.map(query=self.query) \
                .to_sorted_set(key_pattern='zset:{id}', \
                transform=take_nth(1), score='id')
        self.mapper.run()

        self.common_asserts()
        self.redis.pipeline().zadd.assert_any_call('zset:1', 1, 'A')
        self.redis.pipeline().zadd.assert_any_call('zset:2', 2, 'B')

    def test_map_to_sorted_set_by_score_column_number(self):
        self.db = mock_db(('id', 'name', 'salary'), \
                [(1, 'A', 5), (2, 'B', 6), None])
        self.mapper = Db2RedisMapper(self.db, self.redis)

        self.mapper.map(query=self.query) \
                .to_sorted_set(key_pattern='zset:{id}', \
                transform=take_nth(1), score=2)
        self.mapper.run()

        self.common_asserts()
        self.redis.pipeline().zadd.assert_any_call('zset:1', 5, 'A')
        self.redis.pipeline().zadd.assert_any_call('zset:2', 6, 'B')

    def test_map_to_sorted_set_by_score_value(self):
        self.mapper.map(query=self.query) \
                .to_sorted_set(key_pattern='zset:{id}', \
                transform=take_nth(1), score=2.0)
        self.mapper.run()

        self.common_asserts()
        self.redis.pipeline().zadd.assert_any_call('zset:1', 2.0, 'A')
        self.redis.pipeline().zadd.assert_any_call('zset:2', 2.0, 'B')

    def test_map_to_sorted_set_by_score_function(self):
        self.mapper.map(query = self.query) \
                .to_sorted_set(key_pattern='zset:{id}', \
                score = lambda s, e: len(e))
        self.mapper.run()

        self.common_asserts()
        self.redis.pipeline().zadd.assert_any_call('zset:1', 2, 1)
        self.redis.pipeline().zadd.assert_any_call('zset:2', 2, 2)

    def common_asserts(self):
        self.db.cursor().execute.assert_called_with(self.query)
        self.redis.pipeline.assert_called_with()
        self.redis.pipeline().execute.assert_called_with()

