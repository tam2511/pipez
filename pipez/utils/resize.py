from typing import Tuple
import cv2
import numpy as np


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

    image_resized = cv2.resize(image, (w_new, h_new), interpolation=cv2.INTER_AREA)
    image_padded = np.full((size[0], size[1], c), fill_value=pad_value, dtype=image.dtype)

    h_pos, w_pos = (size[0] - h_new) // 2, (size[1] - w_new) // 2
    image_padded[h_pos: h_pos + h_new, w_pos: w_pos + w_new] = image_resized

    return image_padded
