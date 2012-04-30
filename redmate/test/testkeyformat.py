import unittest
from mock import MagicMock
from redmate.keyformat import KeyPattern

class KeyFormatTest(unittest.TestCase):

    test_data = (
        ("justkey", "justkey", (1,), {}),
        ("key:1", "key:{id}", (1,), {"id": 1}),
        ("key:2:1", "key:{1}:{0}", (1, 2), {}),
        ("key:1:0:hi", "key:{id}:{0}:{name}", (0, 1, "hi"), {"id": 1, "name": "hi"})
        )

    def setUp(self):
        self.rows = MagicMock(name="row-iterator-mock")

    def test_standard_patterns(self):
        for case in self.test_data:
            expected = case[0]
            key_pattern = KeyPattern(case[1])
            self.rows.make_dict.return_value = case[3]
            formatted = key_pattern.format(case[2], self.rows)
            self.assertEqual(expected, formatted)
