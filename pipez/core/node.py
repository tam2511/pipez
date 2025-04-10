import logging
import multiprocessing
import threading
import time
from abc import ABC, abstractmethod
from collections import deque
from typing import List, Optional, Union

from .batch import Batch
from .enums import BatchStatus, NodeStatus, NodeType
from .memory import Memory
from .shared_memory import SharedMemory


class Node(ABC):
    def __init__(
            self,
            node_type: NodeType = NodeType.THREAD,
            input: Optional[Union[str, List[str]]] = None,
            output: Optional[Union[str, List[str]]] = None,
            timeout: float = 0.0
    ):
        self.name = self.__class__.__name__

        if node_type == NodeType.PROCESS and (input or output):
            raise RuntimeError

        self.status = NodeStatus.PENDING

        if node_type == NodeType.THREAD:
            self.worker = threading.Thread(target=self.run,
                                           name=self.name,
                                           daemon=True)
        elif node_type == NodeType.PROCESS:
            self.worker = multiprocessing.get_context('spawn').Process(target=self.run,
                                                                       name=self.name,
                                                                       daemon=True)

        self.input = [] if input is None else [input] if isinstance(input, str) else input
        self.output = [] if output is None else [output] if isinstance(output, str) else output
        self.input_queues = []
        self.output_queues = []

        self.memory = Memory()
        self.shared_memory = SharedMemory.get_shared_memory()
        self.metrics = dict(time=deque([0.0], maxlen=100), input=0, output=0)
        self.timeout = timeout

    @property
    def is_pending(self) -> bool:
        return self.status == NodeStatus.PENDING

    @property
    def is_alive(self) -> bool:
        return self.status == NodeStatus.ALIVE

    @property
    def is_completed(self) -> bool:
        return self.status == NodeStatus.COMPLETED

    @property
    def is_terminated(self) -> bool:
        return self.status == NodeStatus.TERMINATED

    def start(self):
        if self.status != NodeStatus.PENDING:
            raise RuntimeError

        self.status = NodeStatus.ALIVE
        self.worker.start()

    def join(self):
        self.worker.join()

    def terminate(self):
        if self.status != NodeStatus.ALIVE:
            raise RuntimeError

        for queue in self.input_queues + self.output_queues:
            while not queue.empty():
                queue.get()

        if isinstance(self.worker, multiprocessing.context.SpawnProcess):
            self.worker.terminate()

        self.release()
        self.status = NodeStatus.TERMINATED

    def post_init(self):
        pass

    def release(self):
        pass

    def run(self):
        self.post_init()

        while self.status == NodeStatus.ALIVE:
            if not self.input_queues:
                input = None
            elif len(self.input_queues) == 1:
                input = self.input_queues[0].get()
            else:
                batches = [queue.get() for queue in self.input_queues]

                if len(set(len(batch) for batch in batches)) > 1:
                    logging.error(f'{self.name}: BatchLengthMismatchError')
                    self.status = NodeStatus.TERMINATED
                    break

                if all(batch.is_ok for batch in batches):
                    status = BatchStatus.OK
                elif all(batch.is_last for batch in batches):
                    status = BatchStatus.LAST
                else:
                    logging.error(f'{self.name}: BatchStatusMismatchError')
                    self.status = NodeStatus.TERMINATED
                    break

                input = Batch(status=status)

                for idx in range(len(batches[0])):
                    input.append({
                        queue.name: batch[idx]
                        for queue, batch in zip(self.input_queues, batches)
                    })

                for batch in batches:
                    input.metadata.update(batch.metadata)

            try:
                start_time = time.perf_counter()
                output = self.processing(input)
                end_time = time.perf_counter()
                self.metrics['time'].append(end_time - start_time)
                self.metrics['input'] += len(input) if isinstance(input, Batch) else 0
                self.metrics['output'] += len(output) if isinstance(output, Batch) else 0
            except Exception as e:
                logging.error(f'{self.name}: {e.__class__} {e}', exc_info=True)
                self.status = NodeStatus.TERMINATED
                break

            if (
                (output is None and self.output_queues) or
                (isinstance(output, Batch) and not self.output_queues)
            ):
                logging.error(f'{self.name}: NodeOutputMismatchError')
                self.status = NodeStatus.TERMINATED
                break

            if (
                isinstance(input, Batch) and
                isinstance(output, Batch) and
                ((input.is_ok and output.is_last) or (input.is_last and output.is_ok))
            ):
                logging.error(f'{self.name}: BatchStatusMismatchError')
                self.status = NodeStatus.TERMINATED
                break

            if isinstance(output, Batch):
                for queue in self.output_queues:
                    queue.put(output)

            if (
                (isinstance(input, Batch) and input.is_last) or
                (isinstance(output, Batch) and output.is_last)
            ):
                self.release()
                self.status = NodeStatus.COMPLETED
                break

            time.sleep(self.timeout)

    @abstractmethod
    def processing(self, data: Optional[Batch]) -> Optional[Batch]:
        pass
