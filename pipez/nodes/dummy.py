from typing import Optional
import random

from pipez.node import Node
from pipez.batch import Batch, BatchStatus
from pipez.registry import Registry


Registry.add
class DummyNode(Node):
    def __init__(
            self,
            p_end: float = 0.1,
            **kwargs
    ):
        super().__init__(**kwargs)

        self._p = p_end

    def work_func(
            self,
            data: Optional[Batch] = None
    ) -> Batch:
        if random.random() < self._p:
            return Batch(status=BatchStatus.END)
        return Batch()
