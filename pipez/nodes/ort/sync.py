from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any
import numpy as np

from .base import BaseORT
from pipez.core.batch import Batch


class SyncORT(BaseORT, ABC):
    def __init__(
            self,
            data_key: Optional[str] = None,
            **kwargs
    ):
        super().__init__(**kwargs)
        self._data_key = data_key

    def processing(self, data: Optional[Batch]) -> Optional[Batch]:
        images = []
        metas = []

        for obj in data:
            image, meta = self._preprocessing(obj[self._data_key] if isinstance(obj, dict) else obj)
            images.append(image)
            metas.append(meta)

        batch = Batch(meta=data.meta)

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
