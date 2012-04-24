import unittest
from testmapper import MapperTest
from testdb import DbAdapterTest

def all_tests():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(DbAdapterTest))
    suite.addTest(unittest.makeSuite(MapperTest))
    return suite
