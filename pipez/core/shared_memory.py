from multiprocessing import Manager
from multiprocessing.managers import DictProxy


class SharedMemory:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
            cls._instance._shared_memory = Manager().dict()

        return cls._instance

    @property
    def shared_memory(self) -> DictProxy:
        return self._shared_memory
