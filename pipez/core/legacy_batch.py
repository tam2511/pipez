from enum import Enum, auto
from typing import Dict, List, Optional, Any


class BatchStatus(Enum):
    OK = auto()
    ERROR = auto()
    END = auto()
    SKIP = auto()


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

    def is_skip(
            self
    ) -> bool:
        return self._status == BatchStatus.SKIP

    @property
    def meta(
            self
    ) -> Dict:
        return self._meta

    def extend(
            self,
            batch: Optional['Batch'] = None
    ) -> None:
        if batch is None:
            return
        self._data.extend(batch.data)
        if isinstance(self._meta, dict):
            self._meta['size'] = len(self)
        if isinstance(batch.meta, dict):
            batch.meta['size'] = len(batch)
        self._meta = [self._meta] if isinstance(self._meta, dict) else self._meta
        if len(self) == 0:
            self._meta = []
        self._meta.extend(
            [batch.meta] if isinstance(batch.meta, dict) else batch.meta
        )
