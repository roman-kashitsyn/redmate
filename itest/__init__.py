import unittest
from mysqltest import MySqlMappingTest

def integration_tests():
    test_cases = [MySqlMappingTest]
    suite = unittest.TestSuite()
    for test_case in test_cases:
        suite.addTest(unittest.makeSuite(test_case))
    return suite
