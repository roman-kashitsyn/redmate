class ToHashRule:

    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs

    def run(self, db, redis):
        rows = None
        if "table" in self.kwargs:
            rows = db.select(
                "select * from {0}".format(self.kwargs["table"]), as_hash=True)
        if not rows:
            return
        key = self.kwargs["key_pattern"]
        pipe = redis.pipeline()
        for row in rows:
            pipe.hmset(key, row)
        pipe.execute()

    
