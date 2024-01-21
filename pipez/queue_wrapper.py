from typing import Union, Any
from queue import Queue as tQueue
from multiprocessing.queues import Queue as mQueue


class QueueWrapper:
    def __init__(
            self,
            name: str,
            queue: Union[mQueue, tQueue]
    ) -> None:
        self._name = name
        self._queue = queue

    @property
    def name(self) -> str:
        return self._name

    def get(self, *args, **kwargs) -> Any:
        return self._queue.get(*args, **kwargs)

    def put(self, *args, **kwargs) -> None:
        return self._queue.put(*args, **kwargs)

    def empty(self) -> bool:
        return self._queue.empty()
