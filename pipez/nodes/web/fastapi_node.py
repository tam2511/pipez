from typing import Optional, Callable, List

from fastapi import FastAPI, APIRouter
from pydantic import BaseModel
import uvicorn

from pipez.node import Node
from pipez.batch import Batch


class FastAPINode(Node):
    def __init__(
            self,
            host: str = '127.0.0.1',
            port: int = 8888,
            **kwargs
    ):
        super().__init__(**kwargs)

        self._host = host
        self._port = port

        self._app = FastAPI()
        self._router = APIRouter()
        self._is_run = False

    def inner_post_init(self):
        self._app.include_router(self._router)
        uvicorn.run(self._app, host=self._host, port=self._port)
        self._is_run = True

    def _add_route(
            self,
            rout_path: str,
            end_point: Callable,
            response_model: Optional[BaseModel],
            methods: List[str]
    ):
        self._router.add_api_route(
            rout_path,
            end_point,
            response_model=response_model,
            methods=methods
        )

    def work_func(
            self,
            data: Optional[Batch] = None
    ) -> Batch:
        if not self._is_run:
            self.inner_post_init()

        return Batch()
