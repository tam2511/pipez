from typing import Optional
from pipez.core.batch import Batch
from pipez.core.node import Node
from pipez.core.registry import Registry


@Registry.add
class Ungroup(Node):
    """
    Узел разгруппировки данных пакета
    """
    def __init__(
            self,
            class_name: str,
            **kwargs
    ):
        super().__init__(name=self.__class__.__name__, **kwargs)
        self._class_name = class_name

    def processing(self, data: Optional[Batch]) -> Optional[Batch]:
        batch = Batch(meta=data.meta)
        batch.meta.update(idxs=[], batch_size=len(data))

        for idx, obj in enumerate(data):
            for crop in obj.get(self._class_name, []):
                batch.append(crop['crop'])
                batch.meta['idxs'].append(idx)

        return batch
