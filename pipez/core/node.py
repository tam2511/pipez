import logging
import time
from abc import ABC, abstractmethod
from collections import deque
from multiprocessing import Process
from multiprocessing.managers import DictProxy
from threading import Thread
from typing import Dict, List, Optional, Union

from .batch import Batch
from .enums import BatchStatus, NodeStatus, NodeType
from .memory import Memory
from .queue import Queue
from .shared_memory import SharedMemory


class Node(ABC):
    def __init__(
            self,
            node_type: NodeType = NodeType.THREAD,
            input: Optional[Union[str, List[str]]] = None,
            output: Optional[Union[str, List[str]]] = None,
            timeout: float = 0.0
    ):
        self._name = self.__class__.__name__
        self._node_type = node_type
        self._status = NodeStatus.PENDING

        self._input = [] if input is None else [input] if isinstance(input, str) else input
        self._output = [] if output is None else [output] if isinstance(output, str) else output
        self._input_queues = []
        self._output_queues = []

        self._memory = Memory()
        self._shared_memory = SharedMemory.get_shared_memory()
        self._metrics = dict(execution_time=deque([0.0], maxlen=100), input=0, output=0)
        self._timeout = timeout

    @property
    def name(self) -> str:
        return self._name

    @property
    def node_type(self) -> NodeType:
        return self._node_type

    @property
    def is_pending(self) -> bool:
        return self._status == NodeStatus.PENDING

    @property
    def is_alive(self) -> bool:
        return self._status == NodeStatus.ALIVE

    @property
    def is_completed(self) -> bool:
        return self._status == NodeStatus.COMPLETED

    @property
    def is_terminated(self) -> bool:
        return self._status == NodeStatus.TERMINATED

    @property
    def input(self) -> List[str]:
        return self._input

    @property
    def output(self) -> List[str]:
        return self._output

    @property
    def input_queues(self) -> List[Queue]:
        return self._input_queues

    @property
    def output_queues(self) -> List[Queue]:
        return self._output_queues

    @property
    def memory(self) -> Memory:
        return self._memory

    @property
    def shared_memory(self) -> DictProxy:
        return self._shared_memory

    @property
    def metrics(self) -> Dict:
        return self._metrics

    def start(self):
        if self._status != NodeStatus.PENDING:
            raise RuntimeError

        self._status = NodeStatus.ALIVE

        if self._node_type == NodeType.THREAD:
            Thread(target=self._run, name=self._name).start()
        elif self._node_type == NodeType.PROCESS:
            Process(target=self._run, name=self._name).start()

    def terminate(self):
        self._status = NodeStatus.TERMINATED

        for queue in self._input_queues + self._output_queues:
            while not queue.empty():
                queue.get()

    def post_init(self):
        pass

    def release(self):
        pass

    def _run(self):
        self.post_init()

        while self._status == NodeStatus.ALIVE:
            if not self._input_queues:
                input = None
            elif len(self._input_queues) == 1:
                input = self._input_queues[0].get()
            else:
                batches = [queue.get() for queue in self._input_queues]

                if any(len(batch) != len(batches[0]) for batch in batches):
                    logging.error(f'{self.name}: BatchLengthMismatchError')
                    self._status = NodeStatus.TERMINATED
                    break

                if all(batch.is_ok for batch in batches):
                    status = BatchStatus.OK
                elif all(batch.is_last for batch in batches):
                    status = BatchStatus.LAST
                else:
                    logging.error(f'{self.name}: BatchStatusMismatchError')
                    self._status = NodeStatus.TERMINATED
                    break

                input = Batch(status=status)

                for idx in range(len(batches[0])):
                    input.append({
                        queue.name: batch[idx]
                        for queue, batch in zip(self._input_queues, batches)
                    })

                for batch in batches:
                    input.metadata.update(batch.metadata)

            try:
                start_time = time.perf_counter()
                output = self.processing(input)
                end_time = time.perf_counter()
                self._metrics['execution_time'].append(end_time - start_time)
                self._metrics['input'] += len(input) if isinstance(input, Batch) else 0
                self._metrics['output'] += len(output) if isinstance(output, Batch) else 0
            except Exception as e:
                logging.error(f'{self.name}: {e.__class__} {e}', exc_info=True)
                self._status = NodeStatus.TERMINATED
                break

            if (
                (output is None and self._output_queues) or
                (isinstance(output, Batch) and not self._output_queues)
            ):
                logging.error(f'{self.name}: NodeOutputMismatchError')
                self._status = NodeStatus.TERMINATED
                break

            if (
                isinstance(input, Batch) and
                isinstance(output, Batch) and
                ((input.is_ok and output.is_last) or (input.is_last and output.is_ok))
            ):
                logging.error(f'{self.name}: BatchStatusMismatchError')
                self._status = NodeStatus.TERMINATED
                break

            if isinstance(output, Batch):
                for queue in self._output_queues:
                    queue.put(output)

            if (
                (isinstance(input, Batch) and input.is_last) or
                (isinstance(output, Batch) and output.is_last)
            ):
                self._status = NodeStatus.COMPLETED
                break

            time.sleep(self._timeout)

        self.release()

    @abstractmethod
    def processing(self, data: Optional[Batch]) -> Optional[Batch]:
        pass
