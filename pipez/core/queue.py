import multiprocessing
import queue

from .batch import Batch
from .enums import NodeType


class Queue:
    def __init__(
            self,
            name: str,
            node_type: NodeType = NodeType.THREAD,
            maxsize: int = 0
    ):
        self._name = name

        if node_type == NodeType.THREAD:
            self._queue = queue.Queue(maxsize=maxsize)
        elif node_type == NodeType.PROCESS:
            self._queue = multiprocessing.Queue(maxsize=maxsize)

    @property
    def name(self) -> str:
        return self._name

    def empty(self) -> bool:
        return self._queue.empty()

    def put(self, item: Batch) -> None:
        self._queue.put(item)

    def get(self) -> Batch:
        return self._queue.get()
