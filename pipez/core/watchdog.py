from typing import Optional, List
from fastapi import FastAPI, APIRouter, Request
# from fastapi.templating import Jinja2Templates
# from fastapi.staticfiles import StaticFiles
# from fastapi.responses import HTMLResponse
# from datetime import datetime
import uvicorn
import logging
# import os

from pipez.core.batch import Batch
from pipez.core.enums import BatchStatus
from pipez.core.node import Node
from pipez.core.queue_wrapper import QueueWrapper


class Watchdog(Node):
    def __init__(
            self,
            pipeline: List[Node],
            *,
            verbose_metrics: bool = False,
            metrics_host: str = '0.0.0.0',
            metrics_port: int = 8887,
            **kwargs
    ):
        super().__init__(name=self.__class__.__name__, timeout=0.1, **kwargs)
        logging.getLogger().setLevel(logging.INFO)
        self._pipeline = pipeline
        self._build_pipeline()
        self.start()

        # if verbose_metrics:
            # self._request = Request
            # self._templates = Jinja2Templates(directory=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates'))

            # app = FastAPI()
            # app.mount(path='/static',
            #           app=StaticFiles(directory=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static'), html=True),
            #           name='static')
            #
            # router = APIRouter()
            # router.add_api_route(path='/metrics', endpoint=self._print_metrics, methods=['GET'], response_class=HTMLResponse)
            # router.add_api_route(path='/metrics_api', endpoint=self._print_metrics_api, methods=['GET'])
            #
            # app.include_router(router)
            # uvicorn.run(app, host=metrics_host, port=metrics_port)

    # def _print_metrics(self, request: Request):
    #     return self._templates.TemplateResponse('home.html', dict(request=request))

    # def _metrics_api(self):
    #     for node in self._pipeline:
    #         node.met
    #
    #     return dict(result=True, current_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), metrics=metrics)



    # def _print_metrics_api(
    #         self,
    #         request: 'Request'
    # ):
    #     message = []
    #     for node in self._pipeline:
    #         metrics = node.metrics
    #         input_processed = metrics.sum('input_processed')
    #         output_processed = metrics.sum('output_processed')
    #         message.append(dict(name=f'{node.name}',
    #                             metrics_sum=str(input_processed) if input_processed > output_processed else str(output_processed),
    #                             metrics_mean=f"{metrics.mean('duration', unit_ms=True):.2f}",
    #                             metrics_std=f"{metrics.std('duration', unit_ms=True):.2f}"))
    #     now = datetime.now()
    #     current_time = now.strftime("%Y-%m-%d %H:%M:%S")
    #     return dict(result=True, current_time=current_time, metrics=message)

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
        if all(node.is_finish for node in self._pipeline):
            for node in self._pipeline:
                node.exit()  # TODO: rename

            logging.info(f'{self.name}: All nodes finished successfully')
            return Batch(status=BatchStatus.END)

        elif any(node.is_terminate for node in self._pipeline):
            logging.info(f'{self.name}: At least one of the nodes has terminated')

            for node in self._pipeline:
                node.exit()  # TODO: rename
                node.drain()
                logging.info(f'{node.name}: Draining')

            return Batch(status=BatchStatus.END)
