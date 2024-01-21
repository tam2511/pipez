from enum import Enum, auto
from typing import Dict, List, Optional, Any


class BatchStatus(Enum):
    OK = auto()
    ERROR = auto()
    END = auto()


class Batch(object):
    def __init__(
            self,
            data: Optional[List[Dict]] = None,
            status: BatchStatus = BatchStatus.OK,
            error: Optional[str] = None,
            meta: Optional[Dict[str, Any]] = None
    ):
        self._data = data if data else []
        self._status = status
        self._error = error
        self._meta = meta.copy() if meta else {}

    def __len__(self):
        return len(self._data)

    def __getitem__(
            self,
            idx: int
    ):
        return self._data[idx]

    def __iter__(self):
        return iter(self._data)

    def append(
            self,
            info: Dict
    ):
        self._data.append(info)

    @property
    def data(
            self
    ) -> List[Dict]:
        return self._data

    @property
    def status(
            self
    ) -> BatchStatus:
        return self._status

    @property
    def error(
            self
    ) -> str:
        return self._error

    def is_ok(
            self
    ) -> bool:
        return self._status == BatchStatus.OK

    def is_error(
            self
    ) -> bool:
        return self._status == BatchStatus.ERROR

    def is_end(
            self
    ) -> bool:
        return self._status == BatchStatus.END

    @property
    def meta(
            self
    ) -> Dict:
        return self._meta
