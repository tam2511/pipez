from math import sqrt


class Metrics(object):
    def __init__(self):
        self._metrics = {}

    def update(self, key, value):
        self._metrics.setdefault(key, []).append(value)

        if key == 'duration' and len(self._metrics[key]) > 1000:
            self._metrics[key] = self._metrics[key][-1000:]

    def mean(self, key, *, unit_ms: bool = False) -> float:
        if key not in self._metrics or not self._metrics[key]:
            return 0

        result = sum(self._metrics[key]) / len(self._metrics[key])
        return result * 1000 if unit_ms else result

    def std(self, key, *, unit_ms: bool = False) -> float:
        if key not in self._metrics or not self._metrics[key]:
            return 0

        data = self._metrics[key]
        average = sum(data) / len(data)
        result = sqrt(sum((x - average) ** 2 for x in data) / len(data))
        return result * 1000 if unit_ms else result

    def sum(self, key) -> int:
        return sum(self._metrics.get(key, []))
