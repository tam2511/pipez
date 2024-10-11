from typing import Optional, List
from threading import Thread
from fastapi import FastAPI, APIRouter, Request
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from statistics import mean, pstdev
from datetime import datetime
from os.path import join, dirname, abspath
import uvicorn
import logging
import time

from pipez.core.batch import Batch
from pipez.core.enums import BatchStatus
from pipez.core.monitoring import Monitoring
from pipez.core.node import Node
from pipez.core.queue_wrapper import QueueWrapper


class Watchdog(Node):
    """
    Наблюдатель за пайплайном
    """
    def __init__(
            self,
            pipeline: List[Node],
            *,
            verbose_metrics: bool = False,
            metrics_host: str = '0.0.0.0',
            metrics_port: int = 8080,
            **kwargs
    ):
        super().__init__(name=self.__class__.__name__, timeout=1.0, **kwargs)
        logging.getLogger().setLevel(logging.INFO)
        self._pipeline = pipeline
        self._build_pipeline()
        self.start()
        Monitoring().start()

        if verbose_metrics:
            self._templates = Jinja2Templates(directory=join(dirname(abspath(__file__)), 'templates'))
            app = FastAPI()
            app.mount('/static', StaticFiles(directory=join(dirname(abspath(__file__)), 'static'), html=True), 'static')
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

                queues[queue] = QueueWrapper(name=queue, type=node.type, maxsize=16)

        for node in self._pipeline:
            for queue in node.input:
                node.input_queue.append(queues[queue])

            for queue in node.output:
                node.output_queue.append(queues[queue])

            node.start()

    def processing(self, data: Optional[Batch]) -> Optional[Batch]:
        self.shared_memory['time'] = time.time()

        if all(node.is_finish for node in self._pipeline):
            logging.info(f'{self.name}: All nodes finished successfully')

            return Batch(status=BatchStatus.END)

        elif any(node.is_terminate for node in self._pipeline):
            logging.info(f'{self.name}: At least one of the nodes has terminated')

            for node in self._pipeline:
                node.drain()
                logging.info(f'{node.name}: Draining')

            return Batch(status=BatchStatus.END)
