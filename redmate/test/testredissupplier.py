import unittest
from mock import Mock
from redmate.supplier import RedisHashSupplier

class RedisHashSupplierTest(unittest.TestCase):

    def setUp(self):
        self.redis_reader = Mock(name="redis-reader-mock")
        self.key_pattern = "key:*"
        self.keys = ("key:1", "key:2")
        self.columns = ("val1", "val2")
        self.redis_reader.keys.return_value = self.keys
        self.values = (
                ("key:1:val:1", "key:1:val:2"),
                ("key:2:val:1", "key:2:val:2"))
        self.redis_reader.read_hash.side_effect = self.values
        self.supplier = RedisHashSupplier(self.key_pattern, self.columns)

    def test_hash_supplier_is_iterable(self):
        vals = None
        with self.supplier(self.redis_reader) as s:
            vals = tuple([x for x in s])
        self.assertEqual(vals, self.values)
        self.redis_reader.keys.assert_called_with(self.key_pattern)
        self.redis_reader.read_hash.assert_any_call(self.keys[0], self.columns)
        self.redis_reader.read_hash.assert_any_call(self.keys[1], self.columns)

    def test_hash_supplier_make_dict(self):
        with self.supplier(self.redis_reader) as s:
            s_it = iter(s)
            dict_1 = s.make_dict(next(s_it))
            self.assertEqual(dict_1, {"val1": "key:1:val:1", "val2": "key:1:val:2"})
            dict_2 = s.make_dict(next(s_it))
            self.assertEqual(dict_2, {"val1": "key:2:val:1", "val2": "key:2:val:2"})

if __name__ == "__main__":
    unittest.main()
