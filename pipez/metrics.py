from typing import Union
from math import sqrt


class Metrics(object):
    _metrics = dict()

    def update(
            self,
            key: str,
            value: Union[int, float]
   ):
        if key not in self._metrics:
            self._metrics[key] = []
        self._metrics[key].append(value)

    def mean(
            self,
            key: str
    ) -> float:
        return sum(self._metrics.get(key, [])) / (len(self._metrics.get(key, [])) + 1e-8)

    def std(
            self,
            key
    ):
        mean = self.mean(key)
        data = self._metrics.get(key, [])
        return sqrt(sum([(x - mean) ** 2 for x in data]) / (len(data) + 1e-8))

    def sum(
            self,
            key
    ):
        return sum(self._metrics.get(key, []))
