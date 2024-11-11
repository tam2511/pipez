import os
import time
import signal
import logging
from datetime import datetime
from statistics import mean, pstdev
from pathlib import Path
import importlib.resources

from typing import Optional, List
from threading import Thread
from fastapi import FastAPI, APIRouter, Request
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import uvicorn

from .batch import Batch
from .enums import NodeType, BatchStatus
from .node import Node
from .queue_wrapper import QueueWrapper
import pipez.resources


class Monitoring(Node):
    def __init__(self, **kwargs):
        super().__init__(node_type=NodeType.PROCESS, timeout=10.0, **kwargs)

    def processing(self, data: Optional[Batch]) -> Optional[Batch]:
        if time.time() - self.shared_memory.get('time', float('inf')) >= 3600.0:
            logging.info(f'{self.name}: os.kill(1, signal.SIGTERM)')
            os.kill(1, signal.SIGTERM)

class Watchdog(Node):
    def __init__(
            self,
            pipeline: List[Node],
            *,
            verbose_metrics: bool = False,
            metrics_host: str = '0.0.0.0',
            metrics_port: int = 8080,
            **kwargs
    ):
        super().__init__(timeout=1.0, **kwargs)
        logging.getLogger().setLevel(logging.INFO)
        self._pipeline = pipeline
        self._build_pipeline()
        self.start()
        Monitoring().start()

        if verbose_metrics:
            directory = Path(importlib.resources.files(pipez.resources)) / 'templates'
            self._templates = Jinja2Templates(directory=directory)
            app = FastAPI()
            directory = Path(importlib.resources.files(pipez.resources)) / 'static'
            app.mount('/static', StaticFiles(directory=directory, html=True), 'static')
            router = APIRouter()
            router.add_api_route('/metrics_html', self._metrics_html, methods=['GET'], response_class=HTMLResponse)
            router.add_api_route('/metrics_json', self._metrics_json, methods=['GET'])
            app.include_router(router)
            Thread(target=uvicorn.run, kwargs=dict(app=app, host=metrics_host, port=metrics_port)).start()

    def _metrics_html(self, request: Request):
        return self._templates.TemplateResponse('home.html', dict(request=request))

    def _metrics_json(self):
        metrics = []

        for node in self._pipeline:
            metrics.append(dict(name=node.name,
                                input=node.metrics['input'],
                                output=node.metrics['output'],
                                duration_mean_ms=f"{mean(node.metrics['duration']) * 1000:.2f}",
                                duration_std_ms=f"{pstdev(node.metrics['duration']) * 1000:.2f}"))

        return dict(current_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), metrics=metrics)

    def _build_pipeline(self):
        queues = {}

        for node in self._pipeline:
            for queue in node.input + node.output:
                if queue in queues:
                    continue

                queues[queue] = QueueWrapper(name=queue, type=node.node_type, maxsize=16)

        for node in self._pipeline:
            for queue in node.input:
                node.input_queues.append(queues[queue])

            for queue in node.output:
                node.output_queues.append(queues[queue])

            node.start()

    def processing(self, data: Optional[Batch]) -> Optional[Batch]:
        self.shared_memory['time'] = time.time()

        if all(node.is_completed for node in self._pipeline):
            logging.info(f'{self.name}: All nodes finished successfully')

            return Batch(status=BatchStatus.LAST)

        elif any(node.is_terminated for node in self._pipeline):
            logging.info(f'{self.name}: At least one of the nodes has terminated')

            for node in self._pipeline:
                node.drain()
                logging.info(f'{node.name}: Draining')

            return Batch(status=BatchStatus.LAST)
