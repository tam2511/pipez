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
        self.session = onnxruntime.InferenceSession(path_or_bytes=model_path, providers=providers)
        self.dtype = dtype
        self.input_name = self.session.get_inputs()[0].name
        self.batch_size, self.channels, self.height, self.width = self.session.get_inputs()[0].shape

        if not isinstance(self.batch_size, int) or self.batch_size < 1:
            self.batch_size = 32

        self.batch = np.zeros((self.batch_size, self.channels, self.height, self.width), dtype=self.dtype)

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

        for i in range(0, len(images), self.batch_size):
            for batch_idx, image in enumerate(images[i: i + self.batch_size]):
                self.batch[batch_idx] = image

            outputs = self.session.run(None, {self.input_name: self.batch})

            for *output, metadata in zip(*outputs, metadatas[i: i + self.batch_size]):
                batch.append(self.postprocessing(output, metadata))

        return batch
