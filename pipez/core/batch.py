from typing import Optional, List, Dict, Any
from pipez.core.enums import BatchStatus


class Batch(object):
    """
    Пакет данных для передачи между узлами
    """
    def __init__(
            self,
            data: Optional[List[Any]] = None,
            meta: Optional[Dict] = None,
            status: BatchStatus = BatchStatus.OK,
            error: Optional[str] = None
    ):
        self._data = data if data else []
        self._meta = meta.copy() if meta else {}
        self._status = status
        self._error = error

    @property
    def data(self) -> List[Any]:
        return self._data

    @property
    def meta(self) -> Dict:
        return self._meta

    @property
    def is_ok(self):
        return self._status == BatchStatus.OK

    @property
    def is_end(self):
        return self._status == BatchStatus.END

    @property
    def is_error(self):
        return self._status == BatchStatus.ERROR

    @property
    def error(self) -> str:
        return self._error

    def __getitem__(self, item) -> Any:
        return self._data[item]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def append(self, data: Any):
        self._data.append(data)
