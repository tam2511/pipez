from typing import List
import logging

from pipez.node import Node, NodeType, NodeStatus
from pipez.batch import Batch, BatchStatus


class WatchDog(Node):
    def __init__(
            self,
            nodes: List[Node],
            **kwargs
    ) -> None:
        super().__init__(name='WatchDog', type=NodeType.THREAD, nodes=nodes, timeout=1e-1)

        self._nodes = self._kwargs['nodes']

    def work_func(
            self,
            data=None
    ) -> Batch:
        if all([node.status == NodeStatus.FINISH for node in self._nodes]):
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
