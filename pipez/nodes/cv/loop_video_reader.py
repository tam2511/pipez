from typing import Optional
import cv2

from pipez.node import Node
from pipez.registry import Registry
from pipez.batch import Batch, BatchStatus


@Registry.add
class LoopVideoReader(Node):
    def __init__(
            self,
            source_memory_key: str,
            result_memory_key: str,
            batch_size: int = 1,
            bgr2rgb: bool = True,
            **kwargs
    ):
        super().__init__(**kwargs)
        self._source_memory_key = source_memory_key
        self._result_memory_key = result_memory_key
        self._batch_size = batch_size
        self._bgr2rgb = bgr2rgb

        self._capture = None
        self._height = None
        self._width = None
        self._fps = None
        self._frame_count = None
        self._frame_duration = None
        self._id = None
        self._in_progress = None
        self._is_open = None
        self._source = None

    def post_init(self):
        self._in_progress = False

    def _start_video_capture(self):
        self._capture = cv2.VideoCapture(self._source)

        if self._capture.isOpened():
            self._height = int(self._capture.get(cv2.CAP_PROP_FRAME_HEIGHT))
            self._width = int(self._capture.get(cv2.CAP_PROP_FRAME_WIDTH))
            self._fps = self._capture.get(cv2.CAP_PROP_FPS)
            self._frame_count = self._capture.get(cv2.CAP_PROP_FRAME_COUNT)
            self._frame_duration = 1000 / self._capture.get(cv2.CAP_PROP_FPS)
            self._in_progress = True
            self._is_open = True
        else:
            self._is_open = False

    def work_func(
            self,
            data: Optional[Batch] = None
    ) -> Batch:
        while not self._in_progress:
            if not len(self.memory[self._source_memory_key]):
                continue

            task = self.memory[self._source_memory_key].pop(0)
            self._source = task['source']
            self._id = task['id']
            self._start_video_capture()

            if self._is_open:
                self.memory[self._result_memory_key][self._id]['num_frames'] = self._frame_count - 1
                self.memory[self._result_memory_key][self._id]['video_height'] = self._height
                self.memory[self._result_memory_key][self._id]['video_width'] = self._width
            else:
                self.memory[self._result_memory_key][self._id]['is_finish'] = True
                self.memory[self._result_memory_key][self._id]['with_error'] = True
                self.memory[self._result_memory_key][self._id]['error'] = "Couldn't open the video"

        batch = Batch()

        while len(batch) < self._batch_size:
            flag, image = self._capture.read()
            current_frame = self._capture.get(cv2.CAP_PROP_POS_FRAMES)

            if current_frame == self._frame_count:
                self._in_progress = False

            if not flag:
                break

            if self._bgr2rgb:
                image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)

            batch.append(dict(image=image,
                              index=round(current_frame) - 1,
                              msec=round(self._capture.get(cv2.CAP_PROP_POS_MSEC))))

        batch.meta.update(dict(source=self._source,
                               id=self._id,
                               batch_size=len(batch),
                               last_batch=False if self._in_progress else True,
                               height=self._height,
                               width=self._width,
                               fps=self._fps,
                               frame_duration=self._frame_duration))

        if not self._in_progress:
            self._capture.release()

        return batch
