from typing import Optional
from pipez.core.batch import Batch
from pipez.core.node import Node
from pipez.core.registry import Registry


@Registry.add
class Group(Node):
    def __init__(
            self,
            class_name: str,
            **kwargs
    ):
        super().__init__(name=self.__class__.__name__, **kwargs)
        self._class_name = class_name

    def processing(self, data: Optional[Batch]) -> Optional[Batch]:
        idxs = iter(data.meta.pop('idxs'))
        batch_size = data.meta.pop('batch_size')
        batch = Batch(data=[{} for _ in range(batch_size)], meta=data.meta)

        for obj in data:
            idx = next(idxs)
            batch[idx].setdefault(self._class_name, []).append(obj)

        return batch
