from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Tuple, Any
import onnxruntime
import numpy as np

from pipez.core.batch import Batch
from pipez.core.node import Node
from pipez.utils.resize import resize


class BaseORT(Node, ABC):
    def __init__(
            self,
            model_path: str,
            providers: Optional[List[str]] = None,
            provider_options: Optional[List[Dict[str, Any]]] = None,
            pad_value: int = 0,
            half_precision: bool = False,
            **kwargs
    ):
        super().__init__(**kwargs)
        self._session = onnxruntime.InferenceSession(path_or_bytes=model_path,
                                                     providers=providers if providers else ['CPUExecutionProvider'],
                                                     provider_options=provider_options)
        self._pad_value = pad_value
        self._dtype = np.float16 if half_precision else np.float32

        net_input = self._session.get_inputs()[0]
        self._input_name = net_input.name
        self._batch_size = net_input.shape[0]

        if self._batch_size == 'batch_size':
            self._batch_size = 32

        if self._batch_size == 'None':
            self._batch_size = 1

        self._size = (net_input.shape[-1], net_input.shape[-2])
        self._num_channels = net_input.shape[1]
        self._output_names = [output.name for output in self._session.get_outputs()]

        self._inputs = np.ones((self._batch_size, self._num_channels, self._size[1], self._size[0]), dtype=self._dtype)
        self._inputs.fill(self._pad_value)

    def _preprocessing(self, image: np.ndarray) -> Tuple[np.ndarray, Dict]:
        orig_shape = image.shape
        image = resize(image=image, size=self._size, pad_value=self._pad_value)
        cur_shape = image.shape
        image = self.preprocessing(image)
        meta = dict(orig_shape=orig_shape, cur_shape=cur_shape)

        return image, meta

    @abstractmethod
    def processing(self, data: Optional[Batch]) -> Optional[Batch]:
        pass

    @abstractmethod
    def preprocessing(self, image: np.ndarray) -> np.ndarray:
        pass

    @abstractmethod
    def postprocessing(self, output: Any, meta: Dict) -> Any:
        pass
