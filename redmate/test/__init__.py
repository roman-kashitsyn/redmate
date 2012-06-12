import unittest
from testkeyformat import KeyFormatTest
from testwriter import RedisWriterTest
from testreader import RedisReaderTest
from testredissupplier import RedisHashSupplierTest
from testdb2redis import Db2RedisMapperTest
from testredis2db import Redis2DbMapperTest

def all_tests():
    test_cases = [KeyFormatTest, RedisWriterTest, RedisReaderTest,
            RedisHashSupplierTest, Db2RedisMapperTest, Redis2DbMapperTest]
    suite = unittest.TestSuite()
    for test_case in test_cases:
        suite.addTest(unittest.makeSuite(test_case))
    return suite
