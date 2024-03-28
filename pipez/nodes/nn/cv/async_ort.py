from abc import ABC, abstractmethod
from typing import Dict, Optional, Tuple, Any
from queue import Queue
from threading import Event
import logging
import numpy as np

from pipez.nodes.nn.cv.base import ORT
from pipez.core.batch import Batch, BatchStatus


class AsyncORT(ORT, ABC):
    def __init__(
            self,
            main_key: str,
            **kwargs
    ):
        super().__init__(**kwargs)
        self._main_key = main_key

    def work_func(
            self,
            data: Optional[Batch] = None
    ) -> Batch:
        try:
            local_queue = Queue()
            event = Event()  # one batch run for one model
            event.set()

            def callback(net_result: np.ndarray, context: Dict, err) -> None:
                batch_results = [
                    tuple(net_result[out_idx][batch_idx] for out_idx in range(len(net_result)))
                    for batch_idx in range(len(net_result[0]))
                ]
                for result, meta in zip(batch_results, context['meta']):
                    result = self.postprocessing(output=result, meta=meta)
                    context['out_queue'].put(dict(output=result))

                context['event'].set()

            images = [self._preprocessing(image=obj[self._main_key]) for obj in data]
            metas = [image[1] for image in images]
            images = [image[0] for image in images]

            for idx in range(0, len(images), self._batch_size):
                event.wait()
                event.clear()

                for batch_idx in range(idx, min(idx + self._batch_size, len(images))):
                    self._inputs[batch_idx - idx] = images[batch_idx]

                self._session.run_async(None,
                                        {self._input_name: self._inputs},
                                        callback,
                                        dict(out_queue=local_queue,
                                             meta=metas[idx: idx + self._batch_size],
                                             event=event))
            event.wait()

            batch_list = []
            while not local_queue.empty():
                batch_list.append(local_queue.get())
            status = BatchStatus.OK
            error = None
        except Exception as e:
            batch_list = []
            logging.error(e)
            status = BatchStatus.ERROR
            error = str(e)
        return Batch(data=batch_list,
                     status=status,
                     error=error,
                     meta=data.meta)

    @abstractmethod
    def preprocessing(
            self,
            image: np.ndarray
    ) -> Tuple[np.ndarray, Dict[str, Any]]:
        pass

    @abstractmethod
    def postprocessing(
            self,
            output: Any,
            meta: Dict[str, Any]
    ) -> Dict[str, Any]:
        pass
