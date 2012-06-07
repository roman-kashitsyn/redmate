class KeyPattern(object):

    def __init__(self, pattern):
        if not pattern:
            raise ValueError("Empty pattern is not a valid pattern")
        self.pattern = pattern

    def format(self, row, rows):
        params = rows.make_dict(row)
        return self.pattern.format(*row, **params)

    def __str__(self):
        return self.pattern
        
