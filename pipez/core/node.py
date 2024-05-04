from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Union
from threading import Thread
from multiprocessing import Process
from collections import deque
import logging
import time

from pipez.core.batch import Batch
from pipez.core.enums import NodeType, NodeStatus, BatchStatus
from pipez.core.memory import Memory
from pipez.core.queue_wrapper import QueueWrapper


class Node(ABC):
    def __init__(
            self,
            name: str,
            type: NodeType = NodeType.THREAD,
            input: Optional[Union[str, List[str]]] = None,
            output: Optional[Union[str, List[str]]] = None,
            timeout: float = 0.0
    ):
        self._name = name
        self._type = type

        if self._type == NodeType.THREAD:
            self._worker = Thread(target=self._run, name=self._name)

        elif self._type == NodeType.PROCESS:
            self._worker = Process(target=self._run, name=self._name)

        self._input = [input] if isinstance(input, str) else input if input else []
        self._output = [output] if isinstance(output, str) else output if output else []
        self._input_queue = []
        self._output_queue = []

        self._memory = Memory()
        self._metrics = dict(duration=deque([0], maxlen=1000), input=0, output=0)
        self._timeout = timeout
        self._status = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def type(self) -> NodeType:
        return self._type

    @property
    def input(self) -> List[str]:
        return self._input

    @property
    def output(self) -> List[str]:
        return self._output

    @property
    def input_queue(self) -> List[QueueWrapper]:
        return self._input_queue

    @property
    def output_queue(self) -> List[QueueWrapper]:
        return self._output_queue

    @property
    def memory(self) -> Memory:
        return self._memory

    @property
    def metrics(self) -> Dict:
        return self._metrics

    @property
    def is_alive(self) -> bool:
        return self._status == NodeStatus.ALIVE

    @property
    def is_finish(self) -> bool:
        return self._status == NodeStatus.FINISH

    @property
    def is_terminate(self) -> bool:
        return self._status == NodeStatus.TERMINATE

    def start(self):
        self._status = NodeStatus.ALIVE
        self._worker.start()

    def _get(self) -> Optional[Batch]:
        if not self._input_queue:
            return None

        if len(self._input_queue) == 1:
            return self._input_queue[0].get()

        batches = [queue.get() for queue in self._input_queue]

        if len(set(len(batch) for batch in batches)) != 1:
            return Batch(status=BatchStatus.ERROR, error='Length batches cannot be different')

        if all(batch.is_ok for batch in batches):
            status = BatchStatus.OK
        elif all(batch.is_end for batch in batches):
            status = BatchStatus.END
        else:
            return Batch(status=BatchStatus.ERROR, error='Batches cannot have different status')

        meta = {}

        for batch in batches:
            meta.update(batch.meta)

        data = [
            {
                queue.name: batch[idx]
                for queue, batch in zip(self._input_queue, batches)
            }
            for idx in range(len(batches[0]))
        ]

        return Batch(data=data, meta=meta, status=status)

    def _step(self, input: Optional[Batch]) -> Optional[Batch]:
        try:
            monotonic = time.monotonic()
            output = self.processing(input)
            self._metrics['duration'].append(time.monotonic() - monotonic)
            self._metrics['input'] += len(input) if isinstance(input, Batch) else 0
            self._metrics['output'] += len(output) if isinstance(output, Batch) else 0
        except Exception as e:
            output = Batch(status=BatchStatus.ERROR, error=f'During processing raise exception {e.__class__} {e}')

        return output

    def _put(self, output: Batch):
        if not self._output_queue:
            return

        for queue in self._output_queue:
            queue.put(output)

    def _run(self):
        while True:
            time.sleep(self._timeout)

            if not self.is_alive:
                break

            input = self._get()

            if isinstance(input, Batch):
                if input.is_end:
                    self._put(input)
                    self._status = NodeStatus.FINISH
                    break
                elif input.is_error:
                    logging.error(f'{self.name}: {input.error}')
                    self._status = NodeStatus.TERMINATE
                    break

            output = self._step(input)

            if isinstance(output, Batch):
                if output.is_ok:
                    self._put(output)
                elif output.is_end:
                    self._put(output)
                    self._status = NodeStatus.FINISH
                    break
                elif output.is_error:
                    logging.error(f'{self.name}: {output.error}')
                    self._status = NodeStatus.TERMINATE
                    break

    def release(self):
        pass

    def drain(self):
        self._status = NodeStatus.TERMINATE

        for queue in self._input_queue + self._output_queue:
            while not queue.empty():
                queue.get()

    @abstractmethod
    def processing(self, data: Optional[Batch]) -> Optional[Batch]:
        pass
