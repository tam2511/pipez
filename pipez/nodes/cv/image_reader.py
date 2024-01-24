from typing import Optional, Union, List
from pathlib import Path

import cv2
import numpy as np

from pipez.node import Node
from pipez.registry import Registry
from pipez.batch import Batch, BatchStatus


__EXTENSIONS__ = ['png', 'jpg', 'jpeg']

def read_image(
        path: str
) -> Optional[np.ndarray]:
    try:
        with open(path, 'rb') as file:
            bytes_image = file.read()
            return cv2.imdecode(np.frombytes(bytes_image, np.uint8), cv2.IMREAD_COLOR)
    except Exception:
        return None


@Registry.add
class ImageReader(Node):
    def __init__(
            self,
            source: str,
            batch_size: int = 1,
            bgr2rgb: bool = False,
            extension: Optional[Union[str, List[str]]] = None,
            **kwargs
    ):
        super().__init__(**kwargs)
        self._source = source
        self._batch_size = batch_size
        self._bgr2rgb = bgr2rgb
        self._extension = __EXTENSIONS__ if extension is None else extension
        self._extension = self._extension if isinstance(self._extension, list) else [self._extension]

        self._iter = None
        self._end = False

    def post_init(self):
        extensions = ''.join([f'[{ext}]' for ext in self._extension])
        self._iter = Path(self._source).rglob(f'*.{extensions}')

    def work_func(
            self,
            data: Optional[Batch] = None
    ) -> Batch:
        batch = Batch()

        while len(batch) < self._batch_size:
            try:
                image_path = next(self._iter)
            except StopIteration:
                break
            image = read_image(image_path)
            if (image is not None) and self._bgr2rgb:
                image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)

            batch.append(dict(
                image=image,
                path=image_path,
                error=image is None
            ))

        if not len(batch):
            return Batch(status=BatchStatus.END)

        batch.meta['batch_size'] = len(batch)

        return batch
