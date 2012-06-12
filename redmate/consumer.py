from keyformat import KeyPattern
import utils

class DbConsumer(object):
    def __init__(self, query):
        self.query = query

    def __call__(self, writer, supplier, entry):
        writer.update(self.query, entry)

    def __str__(self):
        return "DbConsumer(query=" + query + ")"

class RedisConsumer(object):
    def __init__(self, key_pattern, transform=None):
        self.key_pattern = KeyPattern(key_pattern)

        if transform and not callable(transform):
            raise ValueError("Tranfrorm must be a callable")
        self.transform = transform

        if self._need_transform_ and not self.transform:
            self.transform = utils.take_first

    def __call__(self, writer, supplier, entry):
        key = self.key_pattern.format(entry, supplier)
        self._consume(key, writer, supplier, entry)

    def _consume(self, key, writer, supplier, entry):
        raise NotImplementedError

    def _apply_transform(self, entry):
        if self.transform:
            return self.transform(entry)
        else:
            return entry

    def __str__(self):
        return "to_{0}(key_pattern={1})".format(self._name_, self.key_pattern)

class RedisStringConsumer(RedisConsumer):
    _name_ = 'string'
    _need_transform_ = True

    def _consume(self, key, writer, supplier, entry):
        writer.set(key, str(self._apply_transform(entry)))

class RedisHashConsumer(RedisConsumer):
    _name_ = "hash"
    _need_transform_ = False

    def _consume(self, key, writer, supplier, entry):
        hash_value = supplier.make_dict(entry)
        writer.put_to_hash(key, self._apply_transform(hash_value))

class RedisSetConsumer(RedisConsumer):
    _name_ = "set"
    _need_transform_ = True

    def __init__(self, *args, **kwargs):
        super(RedisSetConsumer, self).__init__(*args, **kwargs)

    def _consume(self, key, writer, supplier, entry):
        writer.add_to_set(key, self._apply_transform(entry))

class RedisSortedSetConsumer(RedisConsumer):
    _name_ = "sorted_set"
    _need_transform_ = True

    def __init__(self, key_pattern, score, transform=None):
        super(RedisSortedSetConsumer, self).__init__(key_pattern, transform)
        self.score = score
        self._get_score = self._make_score_func(score)

    def _consume(self, key, writer, supplier, entry):
        score = self._get_score(supplier, entry)
        writer.add_to_sorted_set(key, score, self._apply_transform(entry))

    def _make_score_func(self, score):
        if self._score_is_number():
            return self._get_score_by_column_index
        elif self._score_is_float():
            return lambda s, e: self.score
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

    def _get_score_by_column_name(self, supplier, entry):
        with_cols = supplier.make_dict(entry)
        return with_cols[self.score]

    def _get_score_by_column_index(self, supplier, entry):
        return entry[self.score]


class RedisListConsumer(RedisConsumer):
    _name_ = "list"
    _need_transform_ = True

    def _consume(self, key, writer, supplier, entry):
        writer.add_to_list(key, self._apply_transform(entry))

