from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, Union

import cv2
import numpy as np
import onnxruntime

from ..core.batch import Batch
from ..core.node import Node


def resize(
        image: np.ndarray,
        size: Tuple[int, int],
        pad_value: int = 0
) -> np.ndarray:
    h, w, c = image.shape

    if (h, w) == (size[0], size[1]):
        return image

    ratio = min(size[0] / h, size[1] / w)
    h_new, w_new = round(h * ratio), round(w * ratio)

    image_resized = cv2.resize(image, (0, 0), fx=ratio, fy=ratio, interpolation=cv2.INTER_AREA)
    image_padded = np.full((size[0], size[1], c), fill_value=pad_value, dtype=image.dtype)

    h_pos, w_pos = (size[0] - h_new) // 2, (size[1] - w_new) // 2
    image_padded[h_pos: h_pos + h_new, w_pos: w_pos + w_new] = image_resized

    return image_padded


class NodeONNXRuntime(Node, ABC):
    def __init__(
            self,
            model_path: str,
            providers: List[Union[str, Tuple[str, Dict]]],
            pad_value: int = 0,
            data_key: Optional[str] = None,
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
        self._data_key = data_key

    def _preprocessing(self, image: np.ndarray) -> Tuple[np.ndarray, Dict]:
        meta = dict(original_shape=image.shape)
        image = resize(image, self._size, self._pad_value)
        meta.update(current_shape=image.shape)
        image = self.preprocessing(image)

        return image, meta

    def processing(self, data: Optional[Batch]) -> Optional[Batch]:
        images = []
        metas = []

        for obj in data:
            image, meta = self._preprocessing(obj[self._data_key] if isinstance(obj, dict) else obj)
            images.append(image)
            metas.append(meta)

        batch = Batch(metadata=data.metadata)

        for i in range(0, len(images), self._batch_size):
            for batch_idx, image in enumerate(images[i: i + self._batch_size]):
                self._batch[batch_idx] = image

            inference = self._session.run(None, {self._input_name: self._batch})

            for *output, meta in zip(*inference, metas[i: i + self._batch_size]):
                batch.append(self.postprocessing(output, meta))

        return batch

    @abstractmethod
    def preprocessing(self, image: np.ndarray) -> np.ndarray:
        pass

    @abstractmethod
    def postprocessing(self, output: List, meta: Dict) -> Any:
        pass
