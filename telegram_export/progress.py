class BaseProgress:
    current = 0
    total = 0
    name = ''
    fields = ['current', 'total', 'name']

    def __init__(self, name='', current=0, total=0):
        self.name = name
        self.current = current
        self.total = total

    def inc(self, count=1):
        self.current += count

    def to_dict(self):
        return dict(filter(lambda x: x[0] in self.fields, self.__dict__.items()))


class MessageProgress(BaseProgress):
    pass


class EntityProgress(BaseProgress):
    pass


class MediaProgress(BaseProgress):
    pass
