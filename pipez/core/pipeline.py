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
from .enums import NodeStatus, NodeType
from .node import Node
from .queue import Queue


class Watchdog(Node):
    def __init__(self, **kwargs):
        super().__init__(node_type=NodeType.PROCESS, timeout=10.0, **kwargs)

    def processing(self, data: Optional[Batch]) -> Optional[Batch]:
        if time.time() - self.shared_memory.get('time', float('inf')) >= 120.0:
            logging.info(f'{self.name}: os.kill(1, signal.SIGTERM)')
            os.kill(1, signal.SIGTERM)

        return None


class Pipeline(Node):
    def __init__(
            self,
            nodes: List[Node],
            *,
            queue_maxsize: int = 8,
            verbose_metrics: bool = False,
            metrics_host: str = '0.0.0.0',
            metrics_port: int = 8080,
            **kwargs
    ):
        super().__init__(timeout=1.0, **kwargs)
        logging.getLogger().setLevel(logging.INFO)
        self.nodes = nodes
        self.queue_maxsize = queue_maxsize
        self.build_pipeline()
        self.start()
        Watchdog().start()

        if verbose_metrics:
            self.templates = Jinja2Templates(directory=importlib.resources.files('pipez.resources') / 'templates')
            app = FastAPI()
            app.mount(path='/static',
                      app=StaticFiles(directory=importlib.resources.files('pipez.resources') / 'static', html=True),
                      name='static')
            router = APIRouter()
            router.add_api_route('/metrics_html', self.metrics_html, methods=['GET'], response_class=HTMLResponse)
            router.add_api_route('/metrics_json', self.metrics_json, methods=['GET'])
            app.include_router(router)
            Thread(target=uvicorn.run, kwargs=dict(app=app, host=metrics_host, port=metrics_port)).start()

    def metrics_html(self, request: Request):
        return self.templates.TemplateResponse('home.html', dict(request=request))

    def metrics_json(self):
        metrics = []

        for node in self.nodes:
            metrics.append(dict(name=node.name,
                                input=node.metrics['input'],
                                output=node.metrics['output'],
                                mean_time=f"{mean(node.metrics['time']) * 1000:.2f}",
                                pstdev_time=f"{pstdev(node.metrics['time']) * 1000:.2f}"))

        return dict(current_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), metrics=metrics)

    def build_pipeline(self):
        queues = {}

        for node in self.nodes:
            for queue in node.input + node.output:
                if queue in queues:
                    continue

                queues[queue] = Queue(name=queue, maxsize=self.queue_maxsize)

        for node in self.nodes:
            for queue in node.input:
                node.input_queues.append(queues[queue])

            for queue in node.output:
                node.output_queues.append(queues[queue])

            node.start()

    def processing(self, data: Optional[Batch]) -> Optional[Batch]:
        self.shared_memory['time'] = time.time()

        if all(node.is_completed for node in self.nodes):
            logging.info(f'{self.name}: AllNodesCompleted')

            self.status = NodeStatus.COMPLETED

        elif any(node.is_terminated for node in self.nodes):
            logging.info(f'{self.name}: AtLeastOneNodeTerminated')

            for node in self.nodes:
                node.terminate()
                logging.info(f'{node.name}: NodeTerminated')

            self.status = NodeStatus.COMPLETED

        return None
