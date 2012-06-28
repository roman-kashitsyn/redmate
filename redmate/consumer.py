from .keyformat import KeyPattern
from . import utils

class DbConsumer(object):
    def __init__(self, writer, query):
        self.writer = writer
        self.query = query

    def __call__(self, message):
        self.writer.update(self.query, message.attributes)

    def __str__(self):
        return 'db(query={0})'.format(self.query)

class RedisConsumer(object):
    def __init__(self, writer, key_pattern, transform=None):
        self.writer = writer
        self.key_pattern = KeyPattern(key_pattern)

        if transform and not callable(transform):
            raise ValueError("Tranfrorm must be a callable")
        self.transform = transform

        if self._need_transform_ and not self.transform:
            self.transform = utils.take_first

    def __call__(self, message):
        key = self.key_pattern.format(message)
        self._consume(key, message)

    def _consume(self, key, message):
        raise NotImplementedError

    def _apply_transform(self, entry):
        if self.transform:
            return self.transform(entry)
        else:
            return entry

    def __str__(self):
        return "redis_{0}(key_pattern={1})".format(self._name_, self.key_pattern)

class RedisStringConsumer(RedisConsumer):
    _name_ = 'string'
    _need_transform_ = True

    def _consume(self, key, message):
        self.writer.set(key, str(self._apply_transform(message.attributes)))

class RedisHashConsumer(RedisConsumer):
    _name_ = "hash"
    _need_transform_ = False

    def _consume(self, key, message):
        self.writer.put_to_hash(key, self._apply_transform(message.as_dict()))

class RedisSetConsumer(RedisConsumer):
    _name_ = "set"
    _need_transform_ = True

    def __init__(self, *args, **kwargs):
        super(RedisSetConsumer, self).__init__(*args, **kwargs)

    def _consume(self, key, message):
        self.writer.add_to_set(key, self._apply_transform(message.attributes))

class RedisSortedSetConsumer(RedisConsumer):
    _name_ = "sorted_set"
    _need_transform_ = True

    def __init__(self, writer, key_pattern, score, transform=None):
        super(RedisSortedSetConsumer, self).__init__(writer, key_pattern, transform)
        self.score = score
        self._get_score = self._make_score_func(score)

    def _consume(self, key, message):
        score = self._get_score(message)
        self.writer.add_to_sorted_set(key, score, self._apply_transform(message.attributes))

    def _make_score_func(self, score):
        if self._score_is_number():
            return self._get_score_by_column_index
        elif self._score_is_float():
            return lambda msg: self.score
        elif type(score) is str:
            return self._get_score_by_column_name
        elif callable(score):
            return score
        else:
            raise ValueError("Score parameter must be number, string or callable")

    def _score_is_number(self):
        return type(self.score) in [type(1), type(1L)]

    def _score_is_float(self):
        return type(self.score) is type(0.0)

    def _get_score_by_column_name(self, message):
        return message.as_dict()[self.score]

    def _get_score_by_column_index(self, message):
        return message.attributes[self.score]


class RedisListConsumer(RedisConsumer):
    _name_ = "list"
    _need_transform_ = True

    def _consume(self, key, message):
        self.writer.add_to_list(key, self._apply_transform(message.attributes))

