from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Tuple, Any, Union
import onnxruntime
import numpy as np

from pipez.core.batch import Batch
from pipez.core.node import Node
from pipez.utils.resize import resize


class BaseORT(Node, ABC):
    """
    Базовый класс узла инференса посредством ONNX Runtime
    """
    def __init__(
            self,
            model_path: str,
            providers: List[Union[str, Tuple[str, Dict]]],
            pad_value: int = 0,
            half_precision: bool = False,
            **kwargs
    ):
        super().__init__(**kwargs)
        self._session = onnxruntime.InferenceSession(path_or_bytes=model_path, providers=providers)
        self._pad_value = pad_value
        self._dtype = np.float16 if half_precision else np.float32

        net_input = self._session.get_inputs()[0]
        self._input_name = net_input.name
        self._batch_size = net_input.shape[0]

        if isinstance(self._batch_size, str):
            self._batch_size = 32

        self._size = (net_input.shape[-1], net_input.shape[-2])
        self._batch = np.ones((self._batch_size, net_input.shape[1], self._size[1], self._size[0]), dtype=self._dtype)

    def _preprocessing(self, image: np.ndarray) -> Tuple[np.ndarray, Dict]:
        meta = dict(original_shape=image.shape)
        image = resize(image, self._size, self._pad_value)
        meta.update(current_shape=image.shape)
        image = self.preprocessing(image)

        return image, meta

    @abstractmethod
    def processing(self, data: Optional[Batch]) -> Optional[Batch]:
        pass

    @abstractmethod
    def preprocessing(self, image: np.ndarray) -> np.ndarray:
        pass

    @abstractmethod
    def postprocessing(self, output: List, meta: Dict) -> Any:
        pass
