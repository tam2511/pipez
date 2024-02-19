from typing import Optional
import cv2

from pipez.node import Node
from pipez.registry import Registry
from pipez.batch import Batch


@Registry.add
class LoopVideoReader(Node):
    def __init__(
            self,
            source_memory_key: str,
            result_memory_key: str,
            batch_size: int = 8,
            skip_frames: int = 0,
            bgr2rgb: bool = True,
            **kwargs
    ):
        super().__init__(**kwargs)
        self._source_memory_key = source_memory_key
        self._result_memory_key = result_memory_key
        self._batch_size = batch_size
        self._skip_frames = skip_frames
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
            self._frame_count = int(self._capture.get(cv2.CAP_PROP_FRAME_COUNT)) - 1
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
                self.memory[self._result_memory_key][self._id]['num_frames'] = self._frame_count
                self.memory[self._result_memory_key][self._id]['video_height'] = self._height
                self.memory[self._result_memory_key][self._id]['video_width'] = self._width
            else:
                self.memory[self._result_memory_key][self._id]['error'] = "Couldn't open the video"
                self.memory[self._result_memory_key][self._id]['with_error'] = True
                self.memory[self._result_memory_key][self._id]['is_finish'] = True

        batch = Batch()
        skipped = 0

        while len(batch) < self._batch_size:
            flag, image = self._capture.read()
            current_frame = int(self._capture.get(cv2.CAP_PROP_POS_FRAMES)) - 1
            current_msec = round(current_frame * self._frame_duration)

            if not flag and current_frame < self._frame_count:
                self._in_progress = False
                self._capture.release()
                return Batch(meta=dict(source=self._source,
                                       id=self._id,
                                       batch_size=0,
                                       last_batch=True,
                                       with_error=True,
                                       current_frame=current_frame,
                                       height=self._height,
                                       width=self._width,
                                       fps=self._fps,
                                       frame_duration=self._frame_duration,
                                       frame_count=self._frame_count))

            if not flag:
                break

            if skipped != self._skip_frames:
                skipped += 1
                continue
            else:
                skipped = 0

            if self._bgr2rgb:
                image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)

            batch.append(dict(image=image,
                              index=current_frame,
                              msec=current_msec))

        if not len(batch):
            self._in_progress = False
            self._capture.release()

        batch.meta.update(dict(source=self._source,
                               id=self._id,
                               batch_size=len(batch),
                               last_batch=False if len(batch) else True,
                               with_error=False,
                               current_frame=current_frame,
                               height=self._height,
                               width=self._width,
                               fps=self._fps,
                               frame_duration=self._frame_duration,
                               frame_count=self._frame_count))

        return batch
