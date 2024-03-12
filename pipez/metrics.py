from typing import Union
from math import sqrt


class Metrics(object):
    def __init__(self):
        self._metrics = {}

    def update(
            self,
            key: str,
            value: Union[int, float]
    ):
        self._metrics.setdefault(key, []).append(value)

    def mean(
            self,
            key: str,
            *,
            unit_ms: bool = False
    ) -> float:
        if key not in self._metrics:
            return 0

        result = sum(self._metrics[key]) / len(self._metrics[key]) if self._metrics[key] else 0

        if unit_ms:
            result *= 1000

        return result

    def std(
            self,
            key: str,
            *,
            unit_ms: bool = False
    ) -> float:
        if key not in self._metrics:
            return 0

        data = self._metrics[key]
        average = sum(data) / len(data) if data else 0
        result = sqrt(sum((x - average) ** 2 for x in data) / len(data))

        if unit_ms:
            result *= 1000

        return result

    def sum(
            self,
            key: str
    ) -> int:
        return sum(self._metrics.get(key, []))
