import queue

from .batch import Batch


class Queue:
    def __init__(
            self,
            name: str,
            maxsize: int = 0
    ):
        self._name = name
        self._queue = queue.Queue(maxsize=maxsize)

    @property
    def name(self) -> str:
        return self._name

    def empty(self) -> bool:
        return self._queue.empty()

    def put(self, item: Batch) -> None:
        self._queue.put(item)

    def get(self) -> Batch:
        return self._queue.get()