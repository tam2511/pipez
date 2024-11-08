from typing import Optional
from ..core.batch import Batch
from ..core.node import Node


class Ungroup(Node):
    """
    Узел разгруппировки данных пакета
    """
    def __init__(
            self,
            class_name: str,
            **kwargs
    ):
        super().__init__(**kwargs)
        self._class_name = class_name

    def processing(self, data: Optional[Batch]) -> Optional[Batch]:
        batch = Batch(metadata=data.metadata)
        batch.metadata.update(idxs=[], batch_size=len(data))

        for idx, obj in enumerate(data):
            for crop in obj.get(self._class_name, []):
                batch.append(crop['crop'])
                batch.metadata['idxs'].append(idx)

        return batch
