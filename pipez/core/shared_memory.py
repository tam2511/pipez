import threading
from multiprocessing import Manager
from multiprocessing.managers import DictProxy


class SharedMemory:
    _shared_memory = None
    _lock = threading.Lock()

    @classmethod
    def get_shared_memory(cls) -> DictProxy:
        with cls._lock:
            if cls._shared_memory is None:
                cls._shared_memory = Manager().dict()

        return cls._shared_memory
