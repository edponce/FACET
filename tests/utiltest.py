import copy


class TestStack:

    _STACK = []

    @classmethod
    def push(cls, item):
        cls._STACK.append(copy.deepcopy(item))

    @classmethod
    def pop(cls):
        return cls._STACK.pop()
