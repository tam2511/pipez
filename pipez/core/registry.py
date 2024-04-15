class Registry(object):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
            cls._instance._registry = {}

        return cls._instance

    def __getitem__(self, item):
        if item not in self._registry:
            raise KeyError(f'Class «{item}» not found in registry, it can be added using @Registry.add')

        return self._registry[item]

    @staticmethod
    def add(cls):
        Registry()._registry[cls.__name__] = cls
        return cls
