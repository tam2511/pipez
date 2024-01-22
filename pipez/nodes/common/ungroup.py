from typing import Optional, Dict, List

from pipez.node import Node
from pipez.registry import Registry
from pipez.batch import Batch


def is_keys_available(
        data: Dict,
        keys: List[str]
):
    for key in keys:
        if not isinstance(data, dict):
            return False

        if key not in data:
            return False

        data = data[key]

    return True


@Registry.add
class Ungroup(Node):
    def __init__(
            self,
            keys: List[str],
            main_key: str,
            **kwargs
    ):
        super().__init__(**kwargs)

        self._keys = keys
        self._main_key = main_key

    def work_func(
            self,
            data: Optional[Batch] = None
    ) -> Batch:
        batch = Batch(meta=data.meta)
        batch.meta['idxs'] = []

        for idx, obj in enumerate(data):
            if not is_keys_available(obj, self._keys):
                continue

            for key in self._keys:
                obj = obj[key]

            for target_obj in obj:
                if self._image_key:
                    batch.append({self._main_key: target_obj[self._main_key]})
                else:
                    batch.append({self._main_key: target_obj})

                batch.meta['idxs'].append(idx)

        return batch
