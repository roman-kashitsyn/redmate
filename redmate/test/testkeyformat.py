import unittest
from redmate.keyformat import KeyPattern
from redmate.messaging import Message

class KeyFormatTest(unittest.TestCase):

    test_data = (
        ("justkey", "justkey", (1,), ('id',)),
        ("key:1", "key:{id}", (1,), ('id',)),
        ("key:2:1", "key:{1}:{0}", (1, 2), ('id', 'val')),
        ("key:1:0:hi", "key:{id}:{0}:{name}", (0, 1, "hi"), ('x','id','name'))
        )

    def test_standard_patterns(self):
        for case in self.test_data:
            expected = case[0]
            key_pattern = KeyPattern(case[1])
            attrs = case[2]
            columns = case[3]
            formatted = key_pattern.format(Message(columns, attrs))
            self.assertEqual(expected, formatted)
