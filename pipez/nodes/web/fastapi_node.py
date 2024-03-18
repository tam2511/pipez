from abc import ABC, abstractmethod
from typing import Optional
from fastapi import FastAPI, APIRouter
import uvicorn

from pipez.node import Node
from pipez.batch import Batch


class FastAPINode(Node, ABC):
    def __init__(
            self,
            host: str = '0.0.0.0',
            port: int = 8888,
            **kwargs
    ):
        super().__init__(**kwargs)
        self._host = host
        self._port = port
        self._app = FastAPI()
        self._router = APIRouter()
        self._is_run = False

    def _run(self):
        self.add_api_routes()
        self._app.include_router(self._router)
        uvicorn.run(self._app, host=self._host, port=self._port)
        self._is_run = True

    def work_func(
            self,
            data: Optional[Batch] = None
    ) -> Batch:
        if not self._is_run:
            self._run()

        return Batch()

    @abstractmethod
    def add_api_routes(self):
        pass
