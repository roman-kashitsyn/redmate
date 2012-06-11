from mock import Mock

def mock_db(column_names, values):
    """
    Creates mock db connection that always returns cursors with
    given data.

    column_names -- column names to use in description
    value -- values to return on fetch from cursor

    >>> db = mock_db(('id', 'name'), [(1, 'X'), (2, 'Y'), None])
    >>> c = db.cursor()
    >>> c.description
    [('id', 'mock'), ('name', 'mock')]
    """
    conn = Mock(name="conn-mock")
    conn.cursor().description = [(name, 'mock') for name in column_names]
    conn.cursor().fetchone.side_effect = values
    return conn

if __name__ == "__main__":
    import doctest
    doctest.testmod()
