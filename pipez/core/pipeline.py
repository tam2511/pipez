import importlib.resources
import logging
import os
import signal
import time
from datetime import datetime
from statistics import mean, pstdev
from threading import Thread
from typing import List, Optional

import uvicorn
from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .batch import Batch
from .enums import BatchStatus, NodeType
from .node import Node
from .queue import Queue


class Watchdog(Node):
    def __init__(self, **kwargs):
        super().__init__(node_type=NodeType.PROCESS, timeout=10.0, **kwargs)

    def processing(self, data: Optional[Batch]) -> Optional[Batch]:
        if time.time() - self.shared_memory.get('time', float('inf')) >= 600.0:
            logging.info(f'{self.name}: os.kill(1, signal.SIGTERM)')
            os.kill(1, signal.SIGTERM)

        return None


class Pipeline(Node):
    def __init__(
            self,
            nodes: List[Node],
            *,
            queue_maxsize: int = 16,
            verbose_metrics: bool = False,
            metrics_host: str = '0.0.0.0',
            metrics_port: int = 8080,
            **kwargs
    ):
        super().__init__(timeout=1.0, **kwargs)
        logging.getLogger().setLevel(logging.INFO)
        self._nodes = nodes
        self._queue_maxsize = queue_maxsize
        self._build_pipeline()
        self.start()
        Watchdog().start()

        if verbose_metrics:
            self._templates = Jinja2Templates(directory=importlib.resources.files('pipez.resources') / 'templates')
            app = FastAPI()
            app.mount(path='/static',
                      app=StaticFiles(directory=importlib.resources.files('pipez.resources') / 'static', html=True),
                      name='static')
            router = APIRouter()
            router.add_api_route('/metrics_html', self._metrics_html, methods=['GET'], response_class=HTMLResponse)
            router.add_api_route('/metrics_json', self._metrics_json, methods=['GET'])
            app.include_router(router)
            Thread(target=uvicorn.run, kwargs=dict(app=app, host=metrics_host, port=metrics_port)).start()

    def _metrics_html(self, request: Request):
        return self._templates.TemplateResponse('home.html', dict(request=request))

    def _metrics_json(self):
        metrics = []

        for node in self._nodes:
            metrics.append(dict(name=node.name,
                                input=node.metrics['input'],
                                output=node.metrics['output'],
                                duration_mean_ms=f"{mean(node.metrics['duration']) * 1000:.2f}",
                                duration_std_ms=f"{pstdev(node.metrics['duration']) * 1000:.2f}"))

        return dict(current_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), metrics=metrics)

    def _build_pipeline(self):
        queues = {}

        for node in self._nodes:
            for queue in node.input + node.output:
                if queue in queues:
                    continue

                queues[queue] = Queue(name=queue, node_type=node.node_type, maxsize=self._queue_maxsize)

        for node in self._nodes:
            for queue in node.input:
                node.input_queues.append(queues[queue])

            for queue in node.output:
                node.output_queues.append(queues[queue])

            node.start()

    def processing(self, data: Optional[Batch]) -> Optional[Batch]:
        self.shared_memory['time'] = time.time()

        if all(node.is_completed for node in self._nodes):
            logging.info(f'{self.name}: All nodes finished successfully')

            return Batch(status=BatchStatus.LAST)

        if any(node.is_terminated for node in self._nodes):
            logging.info(f'{self.name}: At least one of the nodes has terminated')

            for node in self._nodes:
                node.drain()
                logging.info(f'{node.name}: Draining')

            return Batch(status=BatchStatus.LAST)

        return None
