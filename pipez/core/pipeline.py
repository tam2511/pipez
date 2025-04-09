import importlib.resources
import logging
import time
from datetime import datetime
from statistics import mean, pstdev
from threading import Thread
from typing import List

import uvicorn
from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .node import Node
from .queue import Queue


class Pipeline:
    def __init__(
            self,
            nodes: List[Node],
            *,
            queue_maxsize: int = 8,
            expose_metrics: bool = False,
            metrics_host: str = '0.0.0.0',
            metrics_port: int = 8000
    ):
        self.nodes = nodes
        self.queue_maxsize = queue_maxsize
        self.expose_metrics = expose_metrics
        self.metrics_host = metrics_host
        self.metrics_port = metrics_port

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

    def run(self):
        logging.getLogger().setLevel(logging.INFO)
        self.build_pipeline()

        if self.expose_metrics:
            self.templates = Jinja2Templates(directory=importlib.resources.files('pipez.resources') / 'templates')
            app = FastAPI()
            app.mount(path='/static',
                      app=StaticFiles(directory=importlib.resources.files('pipez.resources') / 'static', html=True),
                      name='static')
            router = APIRouter()
            router.add_api_route('/metrics_html', self.metrics_html, methods=['GET'], response_class=HTMLResponse)
            router.add_api_route('/metrics_json', self.metrics_json, methods=['GET'])
            app.include_router(router)
            Thread(target=uvicorn.run, kwargs=dict(app=app, host=self.metrics_host, port=self.metrics_port)).start()

        while True:
            if all(node.is_completed for node in self.nodes):
                logging.info('Pipeline: AllNodesCompleted')
                break

            elif any(node.is_terminated for node in self.nodes):
                logging.info('Pipeline: AnyNodeTerminated')

                for node in self.nodes:
                    try:
                        node.terminate()
                    except RuntimeError:
                        pass
                    # node.join()
                    logging.info(f'{node.name}: NodeTerminated')

                break

            time.sleep(1.0)
