from typing import Optional, Union
import cv2

from pipez.node import Node
from pipez.registry import Registry
from pipez.batch import Batch, BatchStatus


@Registry.add
class VideoReader(Node):
    def __init__(
            self,
            source: Union[int, str],
            batch_size: int = 1,
            bgr2rgb: bool = False,
            **kwargs
    ):
        super().__init__(**kwargs)
        self._source = source
        self._batch_size = batch_size
        self._bgr2rgb = bgr2rgb

        self._capture = None
        self._height = None
        self._width = None
        self._fps = None
        self._is_open = None

    def post_init(self):
        self._capture = cv2.VideoCapture(self._source)

        if self._capture.isOpened():
            self._height = int(self._capture.get(cv2.CAP_PROP_FRAME_HEIGHT))
            self._width = int(self._capture.get(cv2.CAP_PROP_FRAME_WIDTH))
            self._fps = self._capture.get(cv2.CAP_PROP_FPS)
            self._is_open = True
        else:
            self._is_open = False

    def work_func(
            self,
            data: Optional[Batch] = None
    ) -> Batch:
        if not self._is_open:
            return Batch(
                status=BatchStatus.ERROR,
                error=f'Source {self._source} was not opened.'
            )

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
            return Batch(status=BatchStatus.END)

        batch.meta.update(dict(batch_size=len(batch),
                               height=self._height,
                               width=self._width,
                               fps=self._fps))

        return batch

    def close(self):
        self._capture.release()
