import queue

from .batch import Batch


class Queue:
    def __init__(
            self,
            name: str,
            maxsize: int = 0
    ):
        self.name = name
        self.queue = queue.Queue(maxsize=maxsize)

    def empty(self) -> bool:
        return self.queue.empty()

    def put(self, item: Batch) -> None:
        self.queue.put(item)

    def get(self) -> Batch:
        return self.queue.get()
