import unittest
from testmaphash import MapToHashTest
from testmaplist import MapToListTest
from testmapset import MapToSetTest
from testdb import DbAdapterTest

def all_tests():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(DbAdapterTest))
    suite.addTest(unittest.makeSuite(MapToHashTest))
    suite.addTest(unittest.makeSuite(MapToListTest))
    suite.addTest(unittest.makeSuite(MapToSetTest))
    return suite
