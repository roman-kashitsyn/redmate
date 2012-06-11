import unittest
from testkeyformat import KeyFormatTest
from testwriter import RedisWriterTest
from testdb2redis import Db2RedisMapperTest

def all_tests():
    test_cases = [KeyFormatTest, RedisWriterTest, Db2RedisMapperTest]
    suite = unittest.TestSuite()
    for test_case in test_cases:
        suite.addTest(unittest.makeSuite(test_case))
    return suite
