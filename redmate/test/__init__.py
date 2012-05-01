import unittest
from mock import MagicMock
from testmaphash import MapToHashTest
from testmaplist import MapToListTest
from testmapset import MapToSetTest
from testdb import DbAdapterTest
from testkeyformat import KeyFormatTest
from testmapsortedset import MapToSortedSetTest
from testpipeline import PipeliningTest

def mock_row_iterator(rows, cols):
    it_mock = MagicMock(name="row-iterator-mock")
    it_mock.__iter__.return_value = rows
    as_dict = lambda x: dict(zip(cols, x))
    it_mock.make_dict = as_dict
    return it_mock

def all_tests():
    test_cases = [
        DbAdapterTest, KeyFormatTest, PipeliningTest, MapToHashTest,
        MapToSetTest,  MapToListTest, MapToSortedSetTest]
    suite = unittest.TestSuite()
    for test_case in test_cases:
        suite.addTest(unittest.makeSuite(test_case))
    return suite
