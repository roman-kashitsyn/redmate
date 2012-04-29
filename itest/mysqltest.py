import unittest
from ConfigParser import ConfigParser
import MySQLdb as dbapi
import redis
import redmate
import bootstrap

class MySqlMappingTest(unittest.TestCase):

    def setUp(self):
        conf = bootstrap.read_config()
        self.redis = redis.StrictRedis(
            host=conf.get("redis", "host"),
            port=conf.getint("redis", "port"),
            db=conf.getint("redis", "db"))
        conn = dbapi.connect(
            host=conf.get("db", "host"),
            port=conf.getint("db", "port"),
            user=conf.get("db", "user"))
        bootstrap.init_db(conn)
        self.db = redmate.Db(conn)
        self.mapper = redmate.Mapper(self.db, self.redis)

    def test_department_to_hash_mapping(self):
        """
        Departments data should be successfully mapped to redis hashes
        """
        rows = [('1', 'IT'), ('2', 'Marketing'), ('3', 'Sales')]
        self.mapper.to_hash(table="redmate.departments", key_pattern="dep:{id}")
        self.mapper.run()
        
        for row in rows:
            self.assertEqual(row[1], self.redis.hget("dep:" + row[0], "name"))
            self.assertEqual(row[0], self.redis.hget("dep:" + row[0], "id"))

    def test_employees_in_departments_set_mapping(self):
        
