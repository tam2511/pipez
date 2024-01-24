from typing import List
import logging

from pipez.node import Node, NodeType, NodeStatus
from pipez.batch import Batch, BatchStatus


class WatchDog(Node):
    def __init__(
            self,
            nodes: List[Node],
            verbose_metrics: bool = True,
            timeout: float = 1e-1,
            **kwargs
    ) -> None:
        super().__init__(name='WatchDog', type=NodeType.THREAD, nodes=nodes, timeout=timeout, **kwargs)

        self._nodes = self._kwargs['nodes']
        self._verbose_metrics = verbose_metrics

    def _print_metrics(self):
        message = []
        for node in self._nodes:
            metrics = node.metrics
            message.append(
                '{}: {}[{:.2f}+-{:.2f} ms]'.format(
                    node.name,
                    metrics.sum('handled'),
                    metrics.mean('duration') * 1000,
                    metrics.std('duration') * 1000,
                )
            )
        message = '\t'.join(message)
        print('\r', message, flush=True, end='', sep='')

    def work_func(
            self,
            data=None
    ) -> Batch:
        if self._verbose_metrics:
            self._print_metrics()
        if all([node.status == NodeStatus.FINISH for node in self._nodes]):
            for node in self._nodes:
                node.close()
            logging.warning('WatchDog node got all finished nodes. This node will be finished.')
            return Batch(data=list(), status=BatchStatus.END)
        if any([node.status == NodeStatus.TERMINATE for node in self._nodes]):
            logging.warning('WatchDog node got some terminated nodes. This node will be finished.')
            for node in self._nodes:
                node.terminate()
                logging.warning(f'Node {node.name} was terminated by watchdog.')
            return Batch(status=BatchStatus.END)
        return Batch(status=BatchStatus.OK)

    @property
    def nodes(self) -> List[Node]:
        return self._nodes
