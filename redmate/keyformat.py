class KeyPattern(object):

    def __init__(self, pattern):
        if not pattern:
            raise ValueError("Empty pattern is not a valid pattern")
        self.pattern = pattern

    def format(self, message):
        return self.pattern.format(*message.attributes, **message.as_dict())

    def __str__(self):
        return self.pattern

