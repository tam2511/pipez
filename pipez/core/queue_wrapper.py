import queue
import multiprocessing
from typing import Any
from .enums import NodeType


class QueueWrapper:
    def __init__(
            self,
            name: str,
            type: NodeType = NodeType.THREAD,
            maxsize: int = 0
    ):
        self._name = name
        self._queue = queue.Queue(maxsize=maxsize) if type == NodeType.THREAD else multiprocessing.Queue(maxsize=maxsize)

    @property
    def name(self) -> str:
        return self._name

    def get(self, *args, **kwargs) -> Any:
        return self._queue.get(*args, **kwargs)

    def put(self, *args, **kwargs) -> None:
        return self._queue.put(*args, **kwargs)

    def empty(self) -> bool:
        return self._queue.empty()

    def size(self) -> int:
        return self._queue.qsize()
