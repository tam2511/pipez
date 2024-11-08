from typing import Optional
from ..core.batch import Batch
from ..core.node import Node


class Group(Node):
    """
    Узел группировки данных пакета
    """
    def __init__(
            self,
            class_name: str,
            **kwargs
    ):
        super().__init__(**kwargs)
        self._class_name = class_name

    def processing(self, data: Optional[Batch]) -> Optional[Batch]:
        idxs = iter(data.metadata.pop('idxs'))
        batch_size = data.metadata.pop('batch_size')
        batch = Batch(data=[{} for _ in range(batch_size)], metadata=data.metadata)

        for obj in data:
            idx = next(idxs)
            batch[idx].setdefault(self._class_name, []).append(obj)

        return batch
