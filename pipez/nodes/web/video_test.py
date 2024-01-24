from typing import Optional, Callable, List

from pydantic import BaseModel

from pipez.nodes.web import FastAPINode
from pipez.batch import Batch


class Interval(BaseModel):
    start_idx: int
    end_idx: int
    start_ts: float
    end_ts: float

class ClassInfo(BaseModel):
    class_name: str
    intervals: List[Interval]

class Result(BaseModel):
    classes: List[ClassInfo]

class TestVideoAPI(FastAPINode):
    def __init__(
            self,
            shared_source_key: str,
            host: str = '127.0.0.1',
            port: int = 8888,
            **kwargs
    ):
        super().__init__(host=host, port=port, **kwargs)

        self._shared_source_key = shared_source_key

    def post_init(self):
        self._add_route(
            rout_path='/start',
            end_point=self.add_task,
            response_model=None,
            methods=['POST']
        )
        super().post_init()

    def add_task(
            self,
            source: str
    ):
        shared = self.shared()
        if self._shared_source_key not in shared:
            shared[self._shared_source_key] = []
        shared[self._shared_source_key] = shared[self._shared_source_key] + [source]

    def result_task(
            self
    ):
        pass

    def work_func(
            self,
            data: Optional[Batch] = None
    ) -> Batch:
        return Batch()
