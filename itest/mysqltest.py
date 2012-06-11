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
        self.conn = dbapi.connect(
            host=conf.get("db", "host"),
            port=conf.getint("db", "port"),
            user=conf.get("db", "user"))
        bootstrap.init_db(self.conn)
        self.redis.flushdb()
        self.mapper = redmate.Db2RedisMapper(self.conn, self.redis)

    def test_department_to_hash_mapping(self):
        """
        Departments data should be successfully mapped to redis hashes
        """
        rows = [('1', 'IT'), ('2', 'Marketing'), ('3', 'Sales')]
        self.mapper.map("select * from redmate.departments") \
                .to_hash(key_pattern="dep:{id}")
        self.mapper.run()

        for row in rows:
            self.assertEqual(row[1], self.redis.hget("dep:" + row[0], "name"))
            self.assertEqual(row[0], self.redis.hget("dep:" + row[0], "id"))

    def test_employees_in_departments_set_mapping(self):
        """
        Employees in Departments many-to-one relation should be mapped to
        redis set
        """
        emps_by_dep = {'1': {'1', '2'}, '2': {'3', '4'}, '3': {'5'}}
        self.mapper.map(query="select id, department_id from redmate.employees") \
                .to_set(key_pattern="dep:{department_id}:employees")
        self.mapper.run()
        for entry in emps_by_dep.items():
            self.assertEqual(entry[1],
                             self.redis.smembers("dep:{0[0]}:employees".format(entry)))

    def test_employees_by_salary_index(self):
        """
        ``employees-by-salary`` index should be successfully created as redis sorted set
        """
        key="employees-by-salary"
        emps_by_salary = {50000: ['5'], 75000: ['4'], 80000: ['3'], 120000: ['2'],
                          100000: ['1']}
        self.mapper.map("select id, salary from redmate.employees") \
                .to_sorted_set(key_pattern=key, score="salary")
        self.mapper.run()

        for sal in emps_by_salary.items():
            self.assertEqual(sal[1],
                             self.redis.zrangebyscore(key, sal[0] - 1, sal[0] + 1))

