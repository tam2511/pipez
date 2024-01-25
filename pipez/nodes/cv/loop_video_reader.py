from typing import Optional
import cv2

from pipez.node import Node
from pipez.registry import Registry
from pipez.batch import Batch, BatchStatus


@Registry.add
class LoopVideoReader(Node):
    def __init__(
            self,
            shared_source_key: str,
            shared_error_key: str,
            batch_size: int = 1,
            bgr2rgb: bool = False,
            **kwargs
    ):
        super().__init__(**kwargs)
        self._shared_source_key = shared_source_key
        self._shared_error_key = shared_error_key
        self._batch_size = batch_size
        self._bgr2rgb = bgr2rgb

        self._capture = None
        self._is_open = True
        self._in_progress = False
        self._source = None
        self._id = None

    def inner_post_init(self):
        self._capture = cv2.VideoCapture(self._source)

        if not self._capture.isOpened():
            self._is_open = False

    def _try_video(self) -> bool:
        shared = self.shared()
        if self._shared_source_key not in shared:
            shared[self._shared_source_key] = []
        if len(shared[self._shared_source_key]) == 0:
            self._in_progress = False
            return False
        self._source = shared[self._shared_source_key][0]['source']
        self._id = shared[self._shared_source_key][0]['id']
        shared[self._shared_source_key] = shared[self._shared_source_key][1:]
        self.inner_post_init()
        if self._shared_error_key not in shared:
            shared[self._shared_error_key] = []
        if not self._is_open:
            shared[self._shared_error_key] = shared[self._shared_error_key] + [self._source]
            return self._try_video()
        self._in_progress = True
        return True

    def work_func(
            self,
            data: Optional[Batch] = None
    ) -> Batch:
        if not self._in_progress:
            self._try_video()
            return Batch(status=BatchStatus.SKIP)

        batch = Batch()

        while len(batch) < self._batch_size:
            flag, image = self._capture.read()

            if not flag:
                break

            if self._bgr2rgb:
                image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)

            batch.append(dict(
                image=image,
                index=round(self._capture.get(cv2.CAP_PROP_POS_FRAMES) - 1),
                msec=round(self._capture.get(cv2.CAP_PROP_POS_MSEC))
            ))

        if not len(batch):
            self.close()
            return Batch(meta=dict(batch_size=0, last_batch=True, id=self._id))

        batch.meta.update(dict(batch_size=len(batch), last_batch=False, id=self._id))
        return batch

    def close(self):
        self._in_progress = False
        self._capture.release()
