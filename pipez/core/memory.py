import threading
from typing import Any


class Memory:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._data = {}

        return cls._instance

    def __len__(self) -> int:
        return len(self._data)

    def __iter__(self):
        return iter(self._data)

    def __getitem__(self, key: Any) -> Any:
        return self._data[key]

    def __setitem__(self, key: Any, value: Any) -> None:
        self._data[key] = value

    def __delitem__(self, key: Any) -> None:
        del self._data[key]

    def __contains__(self, key: Any) -> bool:
        return key in self._data

    def __str__(self) -> str:
        return str(self._data)

    def keys(self):
        return self._data.keys()

    def values(self):
        return self._data.values()
