"""
Various utility methods.
"""

def take_first(row):
    """Returns first element of the list.

    row -- list to take first element from

    >>> take_first((0, 1, 2))
    0
    >>> take_first([2, 1, 0])
    2
    """
    return row[0]

def take_nth(n):
    """Returns function that takes nth element of a list or a tuple.

    n -- index of the element to take

    >>> third = take_nth(2)
    >>> third([0, 1, 2])
    2

    >>> second = take_nth(1)
    >>> second((0, 1, 2))
    1
    """
    return lambda xs: xs[n]

def flatten(line):
    """Flattens string by replace characters (including new lines)
    with simple spaces.

    line -- line to flatten

    >>> flatten("line\\nwith\\nnew\\nlines")
    'line with new lines'
    >>> flatten("line\\nwith\\nnew\\nlines  and   spaces")
    'line with new lines and spaces'
    """
    return " ".join(line.split())

def truncate(line, max_length=100, suffix="."):
    """Truncates line using specified max length and suffix.
    Leaves line untouched if it's length is lesser than max_lenth
    optional parameter value.

    line -- line to truncate.
    max_length -- maximum result string length (100 by default)
    suffix -- suffix to indicate truncated part ('.' by default)

    >>> truncate('spam spam spam', max_length=8)
    'spam ...'
    >>> truncate("spam spam spam")
    'spam spam spam'
    >>> truncate("spam spam spam", max_length=8, suffix="*")
    'spam ***'
    """
    l = flatten(line)
    if len(l) > max_length:
        return l[:max_length - 3] + suffix * 3
    return l

if __name__ == "__main__":
    import doctest
    doctest.testmod()
