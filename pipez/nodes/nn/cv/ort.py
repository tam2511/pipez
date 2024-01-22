from typing import Dict, Tuple, Any, Optional, List
from queue import Queue
from threading import Event

import numpy as np
import onnxruntime as ort

from pipez.node import Node
from pipez.batch import Batch, BatchStatus
from pipez.utils.resize import resize


class OrtCV(Node):
    def __init__(
            self,
            model_path: str,
            providers: Optional[List[str]],
            pad_value: int = 0,
            **kwargs
    ):
        super().__init__(**kwargs)
        self._model_path = model_path
        self._providers = [provider for provider in ort.get_available_providers()] if providers is None else providers
        self._pad_value = pad_value

        self._session = None
        self._input_name = None
        self._batch_size = None
        self._pad_value = None
        self._size = None
        self._num_channels = None
        self._output_names = None
        self._inputs = None

    def post_init(self):
        self._session = ort.InferenceSession(
            path_or_bytes=self._model_path,
            providers=self._providers
        )

        net_input = self._session.get_inputs()[0]
        self._input_name = net_input.name

        self._batch_size = net_input.shape[0]
        self._size = (net_input.shape[-1], net_input.shape[-2])
        self._num_channels = net_input.shape[1]
        self._output_names = [output.name for output in self._session.get_outputs()]

        self._inputs = np.ones((self._batch_size, self._num_channels, self._size[1], self._size[0]), dtype=np.float32)
        self._inputs.fill(self._pad_value)

    def clean_node(self):
        del self._session

    def _preprocessing(
            self,
            image: np.ndarray
    ) -> Tuple[np.ndarray, Dict[str, Any]]:
        orig_shape = image.shape
        image = resize(image=image, size=self._size, pad_value=self._pad_value)
        cur_shape = image.shape

        image, meta = self.preprocessing(image)

        meta['orig_shape'] = orig_shape
        meta['cur_shape'] = cur_shape

        return image, meta

    def preprocessing(
            self,
            image: np.ndarray
    ) -> Tuple[np.ndarray, Dict[str, Any]]:
        raise NotImplementedError

    def postprocessing(
            self,
            output: Any,
            meta: Dict[str, Any]
    ) -> Dict[str, Any]:
        raise NotImplementedError

    def work_func(
            self,
            data: Optional[Batch] = None
    ) -> Batch:
        try:
            local_queue = Queue()
            event = Event()  # one batch run for one model
            event.set()

            def callback(net_result: np.ndarray, context: Dict, err: str) -> None:
                batch_results = [
                    tuple(net_result[out_idx][batch_idx] for out_idx in range(len(net_result)))
                    for batch_idx in range(len(net_result[0]))
                ]

                for result, meta in zip(batch_results, context['meta']):
                    result = self.postprocessing(output=result, meta=meta)

                    context['out_queue'].put(
                        dict(
                            output=result
                        )
                    )

                context['event'].set()

            images = [self._preprocessing(image=obj['image']) for obj in data]
            metas = [image[1] for image in images]
            images = [image[0] for image in images]

            for idx in range(0, len(images), self._batch_size):
                event.wait()
                event.clear()

                for batch_idx in range(idx, min(idx + self._batch_size, len(images))):
                    self._inputs[batch_idx - idx] = images[batch_idx]

                self._session.run_async(
                    None,
                    {self._input_name: self._inputs},
                    callback,
                    dict(
                        out_queue=local_queue,
                        meta=metas[idx: idx + self._batch_size],
                        event=event
                    )
                )

            event.wait()

            batch_list = []
            while not local_queue.empty():
                batch_list.append(local_queue.get())
            status = BatchStatus.OK
            error = None
        except Exception as e:
            batch_list = []
            status = BatchStatus.ERROR
            error = str(e)
        return Batch(
            data=batch_list,
            status=status,
            error=error,
            meta=data.meta
        )
