import unittest
from testmaphash import MapToHashTest
from testmaplist import MapToListTest
from testdb import DbAdapterTest

def all_tests():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(DbAdapterTest))
    suite.addTest(unittest.makeSuite(MapToHashTest))
    suite.addTest(unittest.makeSuite(MapToListTest))
    return suite
