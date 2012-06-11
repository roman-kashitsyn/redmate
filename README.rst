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

The main concept of the package is *mapper* that can move data from one source
to another. Mapper is driven by a set of mapping *rules*.

-------------------
DB to Redis mapping
-------------------

It is very easy to create a mapper: ::

    # assuming db_conn is a connection obtained via PEP-249 api
    # and redis is a redis client
    mapper = redmate.Db2RedisMapper(db_conn, redis)

Here are some examples of mapping:

- Mapping a query to a string::

      mapper.map(query='select * from Spam').to_string(key_pattern='spam')
      mapper.run()

- Mapping a query to redis hash::

      mapper.map(query="select * from Eggs").to_hash(key_pattern="egg:{id}")
      mapper.run()

- Mapping a query to list::

      mapper.map(query="select id, title from Posts") \
            .to_list(key_pattern = "posts" \
                     transform=lambda row: '-'.join(map(str, row)))
      # will produce list of post headers in form {id}-{title}
      mapper.run()

- Mapping a query to set (One-To-Many relation)::

      mapper.map(query="select id, department_id from Employee") \
            .to_set(key_pattern="dep:{department_id}:employees")
      mapper.run()

- Mapping a query to sorted set::

      mapper.map(query="select id, salary from Employees") \
            .to_sorted_set(key_pattern="emp:salary", score="salary")
      mapper.run()

  ``score`` parameter has advanced behavior: it can be number of column
  in result set (if integer), just value of score (if float), name of
  the column to use as score (if string) or some function that returns
  score by result set entry.

Call of map(...).to_DATA_STRUCTURE(...) methods don't actually execute any
actions, all the work is done inside the ``run()`` method (so don't forget to
call it).

You can find more cases in unit tests and examples.

------------
Requirements
------------

RedMate needs Redis client (redis-py_ will suffice) and database
connection (compatible with `PEP 249`_) to work.

.. _redis-py: https://github.com/andymccurdy/redis-py
.. _PEP 249: http://www.python.org/dev/peps/pep-0249/
