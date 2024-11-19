from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import onnxruntime

from ..core.batch import Batch
from ..core.node import Node


class NodeONNXRuntime(Node, ABC):
    def __init__(
            self,
            model_path: str,
            providers: List[Union[str, Tuple[str, Dict]]],
            dtype: np.dtype = np.float32,
            **kwargs
    ):
        super().__init__(**kwargs)
        self._session = onnxruntime.InferenceSession(path_or_bytes=model_path, providers=providers)
        self._dtype = dtype
        self._input_name = self._session.get_inputs()[0].name
        self._batch_size, self._channels, self._height, self._width = self._session.get_inputs()[0].shape

        if not isinstance(self._batch_size, int) or self._batch_size < 1:
            self._batch_size = 32

        self._batch = np.zeros((self._batch_size, self._channels, self._height, self._width), dtype=self._dtype)

    @abstractmethod
    def preprocessing(self, item: Any) -> Tuple[np.ndarray, Any]:
        pass

    @abstractmethod
    def postprocessing(self, output: List[Union[np.ndarray, np.generic]], metadata: Any) -> Any:
        pass

    def processing(self, data: Optional[Batch]) -> Optional[Batch]:
        if not data:
            return data

        batch = Batch(metadata=data.metadata)
        images, metadatas = zip(*(self.preprocessing(item) for item in data))

        for i in range(0, len(images), self._batch_size):
            for batch_idx, image in enumerate(images[i: i + self._batch_size]):
                self._batch[batch_idx] = image

            outputs = self._session.run(None, {self._input_name: self._batch})

            for *output, metadata in zip(*outputs, metadatas[i: i + self._batch_size]):
                batch.append(self.postprocessing(output, metadata))

        return batch
