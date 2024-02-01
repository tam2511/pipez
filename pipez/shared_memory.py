import logging
from typing import Any
from multiprocessing import current_process

from shared_memory_dict import SharedMemoryDict


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


class SharedMemory(metaclass=SingletonMeta):
    def __init__(
            self,
            shared_process_size: int = 2 ** 28
    ):
        self._memory = {}
        self._shared_process_size = shared_process_size

    def __getitem__(
            self,
            key: str
    ) -> Any:
        if current_process().name == 'MainProcess':
            if key not in self._memory:
                logging.error(
                    f'Object {key} not found in registry. You can add it with @Registry.add.'
                )
                raise KeyError(key)

            return self._memory[key]
        else:
            shared_dict = SharedMemoryDict(
                name='_shared_memory',
                size=self._shared_process_size
            )
            return shared_dict[key]

    def __setitem__(
            self,
            key: str,
            value: Any
    ) -> None:
        if current_process().name == 'MainProcess':
            self._memory[key] = value
        else:
            shared_dict = SharedMemoryDict(
                name='_shared_memory',
                size=self._shared_process_size
            )
            shared_dict[key] = value
