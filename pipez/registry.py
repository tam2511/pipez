import logging


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        return cls.get_instance()

    def get_instance(cls):
        if cls not in cls._instances:
            cls.create_instance()
        return cls._instances[cls]

    def create_instance(cls, *args, **kwargs):
        assert cls not in cls._instances
        i = cls.__new__(cls, *args, **kwargs)
        i.__init__(*args, **kwargs)
        cls._instances[cls] = i
        return i


class Registry(metaclass=SingletonMeta):
    def __init__(
            self
    ):
        self.objs = {}

    def __getitem__(
            self,
            item: str
    ):
        if item not in self.objs:
            logging.error(
                f'Object {item} not found in registry. You can add it with @Registry.add.'
            )
            raise KeyError(item)

        return self.objs[item]

    def add_obj(
            self,
            cls
    ):
        name = cls.__name__
        self.objs[name] = cls

    @staticmethod
    def add(cls):
        registry = Registry.get_instance()
        registry.add_obj(cls)
        return cls
