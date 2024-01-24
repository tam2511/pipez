from typing import Optional, List, Union

from pipez.node import Node
from pipez.registry import Registry
from pipez.batch import Batch


@Registry.add
class Get(Node):
    def __init__(
            self,
            key: Union[str, List[str]],
            **kwargs
    ):
        super().__init__(**kwargs)

        self._key = key if isinstance(key, list) else [key]

    def work_func(
            self,
            data: Optional[Batch] = None
    ) -> Batch:
        new_data = [{key: value for key, value in row.items() if key in self._key} for row in data]
        return Batch(data=new_data, meta=data.meta)
