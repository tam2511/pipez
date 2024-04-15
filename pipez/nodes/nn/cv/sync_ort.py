# from abc import ABC, abstractmethod
# from typing import Optional, Tuple, Dict, Any
# import logging
# import numpy as np
#
# from .base import BaseOrt
# from pipez.core import Batch, BatchStatus
#
#
# class SyncOrt(BaseOrt, ABC):
#     def __init__(
#             self,
#             main_key: str,
#             **kwargs
#     ):
#         super().__init__(**kwargs)
#         self._main_key = main_key
#
#     def processing(self, data: Optional[Batch]) -> Optional[Batch]:
#         try:
#             images = [self._preprocessing(image=obj[self._main_key]) for obj in data]
#             metas = [image[1] for image in images]
#             images = [image[0] for image in images]
#             batch_list = []
#
#             for idx in range(0, len(images), self._batch_size):
#                 for batch_idx in range(idx, min(idx + self._batch_size, len(images))):
#                     self._inputs[batch_idx - idx] = images[batch_idx]
#
#                 net_result = self._session.run(None, {self._input_name: self._inputs})
#                 batch_results = [
#                     tuple(net_result[out_idx][batch_idx] for out_idx in range(len(net_result)))
#                     for batch_idx in range(len(net_result[0]))
#                 ]
#                 for result, meta in zip(batch_results, metas[idx: idx + self._batch_size]):
#                     result = self.postprocessing(output=result, meta=meta)
#                     batch_list.append(dict(output=result))
#
#             status = BatchStatus.OK
#             error = None
#         except Exception as e:
#             batch_list = []
#             logging.error(e)
#             status = BatchStatus.ERROR
#             error = str(e)
#         return Batch(data=batch_list,
#                      status=status,
#                      error=error,
#                      meta=data.meta)
#
#     @abstractmethod
#     def preprocessing(
#             self,
#             image: np.ndarray
#     ) -> Tuple[np.ndarray, Dict[str, Any]]:
#         pass
#
#     @abstractmethod
#     def postprocessing(
#             self,
#             output: Any,
#             meta: Dict[str, Any]
#     ) -> Dict[str, Any]:
#         pass
