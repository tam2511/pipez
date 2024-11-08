from typing import Optional, Dict, Any, List
from .enums import BatchStatus


class Batch:
    def __init__(
            self,
            data: Optional[List[Any]] = None,
            metadata: Optional[Dict] = None,
            status: BatchStatus = BatchStatus.OK,
            error: Optional[str] = None
    ):
        self._data = data.copy() if data else []
        self._metadata = metadata.copy() if metadata else {}
        self._status = status
        self._error = error

    @property
    def metadata(self) -> Optional[Dict]:
        return self._metadata

    @property
    def is_ok(self) -> bool:
        return self._status == BatchStatus.OK

    @property
    def is_last(self) -> bool:
        return self._status == BatchStatus.LAST

    @property
    def is_error(self) -> bool:
        return self._status == BatchStatus.ERROR

    @property
    def error(self) -> Optional[str]:
        return self._error

    def __len__(self) -> int:
        return len(self._data)

    def __iter__(self):
        return iter(self._data)

    def __getitem__(self, index: int) -> Any:
        return self._data[index]

    def __setitem__(self, index: int, value: Any) -> None:
        self._data[index] = value

    def __delitem__(self, index: int) -> None:
        del self._data[index]

    def __str__(self) -> str:
        return str(self._data)

    def append(self, item: Any) -> None:
        self._data.append(item)

    def extend(self, iterable: List[Any]) -> None:
        self._data.extend(iterable)
