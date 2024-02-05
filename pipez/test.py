import time

from pipez.node import  Node
from pipez.registry import Registry
from typing import Optional
from pipez.batch import Batch, BatchStatus

from pipez.build import build_pipeline
from pipez.nodes import DummyNode
from pipez.node import NodeType

@Registry.add
class Test(Node):
    def __init__(
            self,
            a: int = 1,
            **kwargs
    ):
        super().__init__(**kwargs)
        self._a = a

    def post_init(self):
        self._a *= 10

    def close(self):
        self._a = 0

    def work_func(
            self,
            data: Optional[Batch] = None
    ) -> Batch:
        self._a *= 2
        time.sleep(5)
        if self._a < 0 :
            return Batch(status=BatchStatus.END)
        return Batch(data=[dict(a=self._a)])

@Registry.add
class Test1(Node):
    def __init__(
            self,
            a: int = 1,
            **kwargs
    ):
        super().__init__(**kwargs)
        self._a = a

    def post_init(self):
        self._a *= 10

    def close(self):
        self._a = 0

    def work_func(
            self,
            data: Optional[Batch] = None
    ) -> Batch:
        self._a += self._a
        time.sleep(3)
        if self._a < 0 :
            return Batch(status=BatchStatus.END)
        return Batch(data=[dict(a=self._a)])

watchdog = build_pipeline(
    pipeline=[
        Test(
            a=10,
            type=NodeType.THREAD,
            name='n1',
        ),
        Test1(
            a=15,
            type=NodeType.THREAD,
            name='n2',
        ),Test1(
            a=835,
            type=NodeType.THREAD,
            name='n3',
        ),
    ], verbose_metrics=True
)