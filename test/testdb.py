import unittest
from mock import Mock
import redmate

class DbAdapterTest(unittest.TestCase):

    def setUp(self):
        self.conn = Mock(name="db-connection-mock")
        self.db = redmate.Db(self.conn)
        self.cursor = self.conn.cursor.return_value
        self.cursor.description = [("id", "int"), ("col", "varchar(25)")]

    def set_row_data(self, row_data):
        self.cursor.fetchone.side_effect = row_data

    def test_select_rows(self):
        """
        select() call should open a cursor and fetch
        rows from database with given query
        """
        row_data = [(1, 'foo'), (2, 'bar'), None]
        self.set_row_data(row_data)

        query = "select * from T"
        rows = self.db.select(query)
        self.assertEqual(row_data[0], next(rows))
        self.assertEqual(row_data[1], next(rows))
        self.assertRaises(StopIteration, next, rows)
        self.conn.cursor.assert_called_with()
        self.cursor.execute.assert_called_with(query)
        self.cursor.fetchone.assert_called_with()

    def test_select_one_row_with_params(self):
        """
        select() call should open a cursor and fetch
        rows from database with given query and params
        """
        row_data = [(1, 'foo'), None]
        self.set_row_data(row_data)

        query = "select * from t where f=?"
        rows = self.db.select(query, "val")
        self.assertEqual(row_data[0], next(rows))
        self.assertRaises(StopIteration, next, rows)
        self.cursor.execute.assert_called_with(query, "val")
        
    def test_select_one_row_as_hash(self):
        """
        select() call with as_hash key should open
        a cursor and return rows iterator that joins
        column names with column values into a dictionary
        """
        row_data = [(1, 'foo'), (2, 'bar'), None]
        self.set_row_data(row_data)

        query = "select * from t"
        rows = self.db.select(query, as_hash=True)
        
        self.assertEqual({"id": 1, "col": "foo"}, next(rows))
        self.assertEqual({"id": 2, "col": "bar"}, next(rows))
        self.assertRaises(StopIteration, next, rows)
        self.conn.cursor.assert_called_with()
        self.cursor.execute.assert_called_with(query)
