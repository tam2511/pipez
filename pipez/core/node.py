import time
import logging
from abc import ABC, abstractmethod
from collections import deque
from threading import Thread
from multiprocessing import Process
from multiprocessing.managers import DictProxy
from typing import Optional, List, Dict, Union

from .batch import Batch
from .enums import NodeType, NodeStatus, BatchStatus
from .memory import Memory
from .queue_wrapper import QueueWrapper
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
        self._shared_memory = SharedMemory().shared_memory
        self._metrics = dict(duration=deque([0], maxlen=100), input=0, output=0)
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
    def is_active(self) -> bool:
        return self._status == NodeStatus.ACTIVE

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
    def input_queues(self) -> List[QueueWrapper]:
        return self._input_queues

    @property
    def output_queues(self) -> List[QueueWrapper]:
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

        self._status = NodeStatus.ACTIVE

        if self._node_type == NodeType.THREAD:
            Thread(target=self._run, name=self._name).start()
        elif self._node_type == NodeType.PROCESS:
            Process(target=self._run, name=self._name).start()

    def post_init(self):
        pass

    def release(self):
        pass

    def _run(self):
        self.post_init()

        while self._status == NodeStatus.ACTIVE:
            input = self._get()

            if isinstance(input, Batch):
                if input.is_last:
                    self._put(input)
                    self._status = NodeStatus.COMPLETED
                    break
                elif input.is_error:
                    logging.error(f'{self.name}: {input.error}')
                    self._status = NodeStatus.TERMINATED
                    break

            output = self._step(input)

            if isinstance(output, Batch):
                if output.is_ok:
                    self._put(output)
                elif output.is_last:
                    self._put(output)
                    self._status = NodeStatus.COMPLETED
                    break
                elif output.is_error:
                    # logging.error(f'{self.name}: {output.error}')
                    self._status = NodeStatus.TERMINATED
                    break

            time.sleep(self._timeout)

        self.release()

    def _get(self) -> Optional[Batch]:
        if not self._input_queues:
            return None

        if len(self._input_queues) == 1:
            return self._input_queues[0].get()

        batches = [queue.get() for queue in self._input_queues]

        if len(set(len(batch) for batch in batches)) != 1:
            return Batch(status=BatchStatus.ERROR, error='Length batches cannot be different')

        if all(batch.is_ok for batch in batches):
            status = BatchStatus.OK
        elif all(batch.is_end for batch in batches):
            status = BatchStatus.LAST
        else:
            return Batch(status=BatchStatus.ERROR, error='Batches cannot have different status')

        metadata = {}

        for batch in batches:
            metadata.update(batch.metadata)

        data = [
            {
                queue.name: batch[idx]
                for queue, batch in zip(self._input_queues, batches)
            }
            for idx in range(len(batches[0]))
        ]

        return Batch(data=data, metadata=metadata, status=status)

    def _step(self, input: Optional[Batch]) -> Optional[Batch]:
        try:
            monotonic = time.monotonic()
            output = self.processing(input)
            self._metrics['duration'].append(time.monotonic() - monotonic)
            self._metrics['input'] += len(input) if isinstance(input, Batch) else 0
            self._metrics['output'] += len(output) if isinstance(output, Batch) else 0
        except Exception as e:
            logging.error(f'{self.name}: {e.__class__} {e}', exc_info=True)
            output = Batch(status=BatchStatus.ERROR, error=f'During processing raise exception {e.__class__} {e}')

        return output

    def _put(self, output: Batch):
        if not self._output_queues:
            return

        for queue in self._output_queues:
            queue.put(output)

    def drain(self):
        self._status = NodeStatus.TERMINATED

        for queue in self._input_queues + self._output_queues:
            while not queue.empty():
                queue.get()

    @abstractmethod
    def processing(self, data: Optional[Batch]) -> Optional[Batch]:
        pass
