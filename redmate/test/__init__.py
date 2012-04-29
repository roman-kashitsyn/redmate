import unittest
from testmaphash import MapToHashTest
from testmaplist import MapToListTest
from testmapset import MapToSetTest
from testdb import DbAdapterTest

def all_tests():
    test_cases = [DbAdapterTest, MapToHashTest, MapToSetTest, MapToListTest]
    suite = unittest.TestSuite()
    for test_case in test_cases:
        suite.addTest(unittest.makeSuite(test_case))
    return suite
