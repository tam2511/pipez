from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
import numpy as np

from .base import BaseORT
from pipez.core.batch import Batch


class SyncORT(BaseORT, ABC):
    def __init__(
            self,
            main_key: str,
            **kwargs
    ):
        super().__init__(**kwargs)
        self._main_key = main_key

    def processing(self, data: Optional[Batch]) -> Optional[Batch]:
        images = []
        metas = []

        for obj in data:
            image, meta = self._preprocessing(image=obj[self._main_key])
            images.append(image)
            metas.append(meta)

        batch = Batch(meta=data.meta)

        for idx in range(0, len(images), self._batch_size):
            for batch_idx in range(idx, min(idx + self._batch_size, len(images))):
                self._inputs[batch_idx - idx] = images[batch_idx]

            net_result = self._session.run(None, {self._input_name: self._inputs})
            batch_results = [
                tuple(net_result[out_idx][batch_idx] for out_idx in range(len(net_result)))
                for batch_idx in range(len(net_result[0]))
            ]
            for result, meta in zip(batch_results, metas[idx: idx + self._batch_size]):
                result = self.postprocessing(output=result, meta=meta)
                batch.append(dict(output=result))

        return batch

    @abstractmethod
    def preprocessing(self, image: np.ndarray) -> np.ndarray:
        pass

    @abstractmethod
    def postprocessing(self, output: Any, meta: Dict) -> Any:
        pass
