from typing import Optional, List
from uuid import uuid4
from enum import Enum

from pydantic import BaseModel

from pipez.nodes.web import FastAPINode
from pipez.batch import Batch


class TaskStatus(Enum):
    QUEUED = 'queued'
    PROGRESS = 'progress'
    ERROR = 'error'
    FINISHED = 'finished'
    NOT_FOUNDED = 'not founded'


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

class StartResponse(BaseModel):
    id: str
    source: str
    status: str


class TestVideoAPI(FastAPINode):
    def __init__(
            self,
            shared_source_key: str,
            shared_result_key: str,
            host: str = '127.0.0.1',
            port: int = 8888,
            **kwargs
    ):
        super().__init__(host=host, port=port, **kwargs)

        self._shared_source_key = shared_source_key
        self._shared_result_key = shared_result_key

    def post_init(self):
        self._add_route(
            rout_path='/start',
            end_point=self.add_task,
            response_model=StartResponse,
            methods=['POST']
        )
        self._add_route(
            rout_path='/status',
            end_point=self.status_task,
            response_model=StartResponse,
            methods=['GET']
        )
        super().post_init()

    def add_task(
            self,
            source: str
    ):
        id = str(uuid4())
        shared = self.shared()
        if self._shared_source_key not in shared:
            shared[self._shared_source_key] = []
        shared[self._shared_source_key] = shared[self._shared_source_key] + [dict(source=source, id=id)]
        if self._shared_result_key not in shared:
            shared[self._shared_result_key] = {}
        result_dict = shared[self._shared_result_key]
        result_dict[id] = dict(
            source=source,
            progress=dict(current=-1, all=-1),
            status=TaskStatus.QUEUED.value,
            results=dict()
        )
        shared[self._shared_result_key] = result_dict
        return dict(
            id=id,
            source=source,
            status=TaskStatus.QUEUED.value
        ), 201

    def status_task(
            self,
            id: str
    ):
        shared = self.shared()
        if self._shared_result_key not in shared:
            shared[self._shared_result_key] = {}
        result_dict = shared[self._shared_result_key]
        if id not in result_dict:
            return dict(
                id=id,
                source='',
                status=TaskStatus.NOT_FOUNDED.value
            ), 200
        return dict(
            id=id,
            source=shared[self._shared_result_key]['source'],
            status=shared[self._shared_result_key]['status'],
        ), 200

    def work_func(
            self,
            data: Optional[Batch] = None
    ) -> Batch:
        return Batch()
