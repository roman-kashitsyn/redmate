=======
RedMate
=======

RedMate is a set of tools to migrate data from relational database
to Redis key-value store.

------------
Installation
------------

To install the package from source just execute:

::

  python setup.py install

-----
Usage
-----

The main concept of the package is *mapper* that can move data from
one source to another. Mapper is driven by a set of mapping *rules*.
Currently only mapping from relational DB to redis is implemented.

Here are some examples (assuming you have imported redmate package
and created both redis (``redis`` variable) connection and db
connection ``db_conn`` variable):

- Mapping a table to redis hash::

      mapper = redmate.Mapper(redis, redmate.Db(db_conn))
      mapper.to_hash(table="Eggs", key_pattern="egg:{id}")
      mapper.run()

- Mapping a query to list::

      mapper = redmate.Mapper(redis, redmate.Db(db_conn))
      mapper.to_list(query="select id, title from Posts",
                     key_pattern = "posts"
                     transform=lambda row: '-'.join(map(str, row)))
      # will produce list of post headers in form {id}-{title}
      mapper.run()

- Mapping a query to set (One-To-Many relation)::

      mapper = redmate.Mapper(redis, redmate.Db(db_conn))
      mapper.to_set(query="select id, department_id from Employee",
                    key_pattern="dep:{department_id}:employees")
      mapper.run()

- Mapping a query to sorted set::

      mapper = redmate.Mapper(redis, redmate.Db(db_conn))
      mapper.to_sorted_set(query="select id, salary from Employees",
                           key_pattern="emp:salary", score="salary")
      mapper.run()

to_DATA_STRUCTURE methods don't actually execute any actions, all the
work is done inside the ``run()`` method (so don't forget to call it).

You can find more cases in unit tests and examples.

------------
Requirements
------------

RedMate needs Redis client (redis-py_ will suffice) and database
connection (compatible with `PEP 249`_) to work.

.. _redis-py: https://github.com/andymccurdy/redis-py
.. _PEP 249: http://www.python.org/dev/peps/pep-0249/
