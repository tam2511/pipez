from typing import Optional

from pipez.node import Node
from pipez.registry import Registry
from pipez.batch import Batch


@Registry.add
class Group(Node):
    def __init__(
            self,
            class_name: str,
            **kwargs
    ):
        super().__init__(**kwargs)

        self._class_name = class_name

    def work_func(
            self,
            data: Optional[Batch] = None
    ) -> Batch:
        idxs = iter(data.meta.pop('idxs'))

        batch = Batch(
            data=[{} for _ in range(data.meta['batch_size'])],
            meta=data.meta
        )

        for obj in data:
            idx = next(idxs)
            batch[idx].setdefault(self._class_name, []).append(obj['output'])

        return batch
