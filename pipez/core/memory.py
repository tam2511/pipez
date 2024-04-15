class Memory(object):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
            cls._instance._memory = {}

        return cls._instance

    def __getitem__(self, item):
        if item not in self._memory:
            raise KeyError(f'Key «{item}» not found in memory')

        return self._memory[item]

    def __setitem__(self, key, value):
        self._memory[key] = value

    def __delitem__(self, key):
        del self._memory[key]

    def __contains__(self, item):
        return item in self._memory

    def __str__(self):
        return str(self._memory)

    def __del__(self):
        del self._memory
