from typing import Any, Dict, List, Optional

from .enums import BatchStatus


class Batch:
    def __init__(
            self,
            data: Optional[List[Any]] = None,
            metadata: Optional[Dict] = None,
            status: BatchStatus = BatchStatus.OK
    ):
        self.data = data.copy() if data else []
        self.metadata = metadata.copy() if metadata else {}
        self.status = status

    @property
    def is_ok(self) -> bool:
        return self.status == BatchStatus.OK

    @property
    def is_last(self) -> bool:
        return self.status == BatchStatus.LAST

    def __len__(self) -> int:
        return len(self.data)

    def __iter__(self):
        return iter(self.data)

    def __getitem__(self, index: int) -> Any:
        return self.data[index]

    def __setitem__(self, index: int, value: Any) -> None:
        self.data[index] = value

    def __delitem__(self, index: int) -> None:
        del self.data[index]

    def __str__(self) -> str:
        return str(self.data)

    def append(self, item: Any) -> None:
        self.data.append(item)

    def extend(self, iterable: List[Any]) -> None:
        self.data.extend(iterable)
