import uuid

class Message(object):
    def __init__(self, columns, attributes):
        self.columns = columns
        self.attributes = attributes

    def as_dict(self):
        return dict(zip(self.columns, self.attributes))

    def __str__(self):
        return 'Message(columns={0}, attrs={1})'.format(self.columns, self.attributes)

    def __repr__(self):
        return str(self)

class Channel(object):

    def __init__(self, name=None):
        self.name = name if name else str(uuid.uuid4())
        self.subscribers = []
    
    def publish(self, message):
        for subscriber in self.subscribers:
            subscriber(message)
        return self

    def __lshift__(self, message):
        return self.publish(message)

    def add_subscriber(self, subscriber):
        if not callable(subscriber):
            raise ValueError("Subscriber must support callable protocol")
        self.subscribers.append(subscriber)
        return self

    def __rshift__(self, subscriber):
        return self.add_subscriber(subscriber)

    def __str__(self):
        return 'Channel(name=%s)' % self.name

