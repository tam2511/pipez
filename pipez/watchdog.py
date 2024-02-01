from typing import List
import logging

from pipez.node import Node, NodeType, NodeStatus
from pipez.batch import Batch, BatchStatus


from fastapi import FastAPI, APIRouter, Request
import uvicorn
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from datetime import datetime
import jsonify

class WatchDog(Node):
    def __init__(
            self,
            nodes: List[Node],
            verbose_metrics: bool = True,
            timeout: float = 1e-1,
            **kwargs
    ) -> None:
        super().__init__(name='WatchDog', type=NodeType.THREAD, nodes=nodes, timeout=timeout, **kwargs)

        self._nodes = self._kwargs['nodes']
        self._verbose_metrics = verbose_metrics
        #self.request = Request
        self.templates = Jinja2Templates(directory="templates")
        self.router = APIRouter()
        self.router.add_api_route("/metrics", self._print_metrics, methods=["GET"],response_class=HTMLResponse)
        self.router.add_api_route("/metrics1", self._print_metrics1, methods=["GET"])

    def post_init(self):
        app = FastAPI()
        app.include_router(self.router)
        uvicorn.run(app, host='127.0.0.1', port=8888)

    def _print_metrics(self,request: Request):
        message = []
        for node in self._nodes:
            metrics = node.metrics
            message.append(
                f"{node.name}: "
                f"{metrics.sum('handled')}["
                f"{metrics.mean('duration', unit_ms=True):.2f}+-"
                f"{metrics.std('duration', unit_ms=True):.2f} ms]"
            )
        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")

        return self.templates.TemplateResponse(
            "home.html",
            {
                "request": request,
                "message": '123',
                "current_time": current_time
            }
        )
    def _print_metrics1(self,request: Request):
        message = []
        for node in self._nodes:
            metrics = node.metrics
            message.append(
                f"{node.name}: "
                f"{metrics.sum('handled')}["
                f"{metrics.mean('duration', unit_ms=True):.2f}+-"
                f"{metrics.std('duration', unit_ms=True):.2f} ms]"
            )
        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")
        data={}
        data['abc']=current_time
        return {'result': True, 'data': data}

    def work_func(
            self,
            data=None
    ) -> Batch:
        if self._verbose_metrics:
            self._print_metrics()
        if all([node.status == NodeStatus.FINISH for node in self._nodes]):
            for node in self._nodes:
                node.close()
            logging.warning('WatchDog node got all finished nodes. This node will be finished.')
            return Batch(data=list(), status=BatchStatus.END)
        if any([node.status == NodeStatus.TERMINATE for node in self._nodes]):
            logging.warning('WatchDog node got some terminated nodes. This node will be finished.')
            for node in self._nodes:
                node.terminate()
                logging.warning(f'Node {node.name} was terminated by watchdog.')
            return Batch(status=BatchStatus.END)
        return Batch(status=BatchStatus.OK)

    @property
    def nodes(self) -> List[Node]:
        return self._nodes
