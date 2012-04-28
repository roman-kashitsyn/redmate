import unittest
from ConfigParser import ConfigParser
import MySQLdb as dbapi
import redis
import redmate

class MySqlMappingTest(unittest.TestCase):

    def __init__(self):
        conf = ConfigParser()
        conf.read("config.ini")
        self.redis = redis.StrictRedis(
            host=conf.get("redis.host"),
            port=conf.getint("redis.port"),
            db=conf.getint("redis.db"))
        conn = dbapi.connect(
            host=conf.get("db.host"),
            port=conf.getint("db.port")
            db=conf.get("db.user"))
        self.db = redmate.Db(conn)
        self.mapper = redmate.Mapper(self.db, self.redis)

    def test_department_mapping(self):

        rows = [('1', 'IT'), ('2', 'Marketing'), ('3', 'Sales')]
        self.mapper.to_hash(table="redmate.departments", key_pattern="dep:{id}")
        self.mapper.run()
        
        for row in rows:
            self.assertEqual(row[1],
                             self.redis.hget("dep:" + row[0], "name"))
