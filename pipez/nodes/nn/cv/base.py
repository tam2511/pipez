from abc import ABC, abstractmethod
from typing import Optional, List, Tuple, Dict, Any
import onnxruntime
import numpy as np

from pipez.node import Node
from pipez.registry import Registry
from pipez.batch import Batch
from pipez.utils.resize import resize


@Registry.add
class ORT(Node, ABC):
    def __init__(
            self,
            model_path: str,
            providers: Optional[List[str]] = None,
            pad_value: int = 0,
            dynamic_batch_size: int = 32,
            **kwargs
    ):
        super().__init__(**kwargs)
        self._model_path = model_path
        self._providers = providers if providers else [provider for provider in onnxruntime.get_available_providers()]
        self._pad_value = pad_value
        self._dynamic_batch_size = dynamic_batch_size

        self._session = None
        self._input_name = None
        self._batch_size = None
        self._size = None
        self._num_channels = None
        self._output_names = None
        self._inputs = None

    def post_init(self):
        self._session = onnxruntime.InferenceSession(path_or_bytes=self._model_path,
                                                     providers=self._providers)

        net_input = self._session.get_inputs()[0]
        self._input_name = net_input.name
        self._batch_size = net_input.shape[0]

        if self._batch_size == 'batch_size':
            self._batch_size = self._dynamic_batch_size

        if self._batch_size == 'None':
            self._batch_size = 1

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

    @abstractmethod
    def work_func(
            self,
            data: Optional[Batch] = None
    ) -> Batch:
        pass

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
