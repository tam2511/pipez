from typing import List
from datetime import datetime
import logging
import os

from pipez.node import Node, NodeType, NodeStatus
from pipez.batch import Batch, BatchStatus

try:
    from fastapi import FastAPI, APIRouter, Request
    from fastapi.staticfiles import StaticFiles
    import uvicorn
    from fastapi.templating import Jinja2Templates
    from fastapi.responses import HTMLResponse
except ImportError:
    logging.warning('For verbose_metrics you must install FastAPI')


class WatchDog(Node):
    def __init__(
            self,
            nodes: List[Node],
            verbose_metrics: bool = False,
            metrics_host: str = '127.0.0.1',
            metrics_port: int = 8887,
            timeout: float = 1e-1,
            **kwargs
    ):
        super().__init__(name='WatchDog', type=NodeType.THREAD, nodes=nodes, timeout=timeout, **kwargs)
        self._nodes = self._kwargs['nodes']
        self._verbose_metrics = verbose_metrics
        self._metrics_host = metrics_host
        self._metrics_port = metrics_port

        self._request = None
        self._templates = None

    def post_init(self):
        if self._verbose_metrics:
            self._request = Request
            self._templates = Jinja2Templates(directory=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates'))

            router = APIRouter()
            router.add_api_route("/metrics", self._print_metrics, methods=["GET"], response_class=HTMLResponse)
            router.add_api_route("/metrics_api", self._print_metrics_api, methods=["GET"])

            app = FastAPI()
            app.mount(path='/static',
                      app=StaticFiles(directory=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static'), html=True),
                      name='static')
            app.include_router(router)
            uvicorn.run(app, host=self._metrics_host, port=self._metrics_port)

    def _print_metrics(
            self,
            request: 'Request'
    ):
        return self._templates.TemplateResponse(
            "home.html",
            {
                "request": request,
            }
        )

    def _print_metrics_api(
            self,
            request: 'Request'
    ):
        message = []
        for node in self._nodes:
            metrics = node.metrics
            message.append(
                {
                    'name': f"{node.name}",
                    'metrics_sum': f"{metrics.sum('handled')}",
                    'metrics_mean': f"{metrics.mean('duration', unit_ms=True):.2f}",
                    'metrics_std': f"{metrics.std('duration', unit_ms=True):.2f}",
                }
            )
        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")
        return {'result': True, 'current_time': current_time,'metrics': message}

    def work_func(
            self,
            data=None
    ) -> Batch:
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
