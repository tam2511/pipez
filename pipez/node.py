from abc import abstractmethod, ABC
from enum import Enum, auto
from typing import Optional, Union, List
from multiprocessing import Process, Value
from threading import Thread
from time import sleep
import logging


from pypez.batch import Batch, BatchStatus
from pypez.queue_wrapper import QueueWrapper


class StepVerdict(Enum):
    STOP = auto()
    CONTINUE = auto()


class NodeStatus(Enum):
    FINISH = auto()
    TERMINATE = auto()
    ALIVE = auto()


class NodeType(Enum):
    THREAD = auto()
    PROCESS = auto()

    @staticmethod
    def from_string(
            type: str
    ) -> 'NodeType':
        if type.lower() == 'thread':
            return NodeType.THREAD
        elif type.lower() == 'process':
            return NodeType.PROCESS
        else:
            raise ValueError


class Node(ABC):
    def __init__(
            self,
            name: str,
            type: NodeType = NodeType.THREAD,
            max_retries: int = 0,
            max_restart_retries: int = 0,
            timeout: float = 0.0,
            in_queue: Optional[Union[QueueWrapper, List[QueueWrapper]]] = None,
            out_queue: Optional[Union[QueueWrapper, List[QueueWrapper]]] = None,
            **kwargs
    ) -> None:
        self._kwargs = kwargs

        self._name = name
        self._type = type
        self._max_retries = max_retries
        self._max_restart_retries = max_restart_retries
        self._timeout = timeout
        self._in_queue = in_queue
        self._out_queue = out_queue

        self._num_retries = 0
        self._num_restart_retries = 0
        self._status = Value('i', NodeStatus.ALIVE.value)

        self._init_worker()

    def post_init(self):
        return

    def close(self):
        return

    @abstractmethod
    def work_func(
            self,
            data: Optional[Batch] = None
    ) -> Batch:
        raise NotImplementedError

    def _init_worker(
            self
    ):
        if self._type == NodeType.PROCESS:
            self._worker = Process(
                target=self.run,
                name=self._name,
            )
        else:
            self._worker = Thread(
                target=self.run,
                name=self._name,
                daemon=False
            )

    def start(
            self
    ):
        self._worker.start()

    @property
    def worker(
            self
    ) -> Union[Thread, Process]:
        return self._worker

    def get(
            self
    ) -> Optional[Batch]:
        if self._in_queue is None:
            return None
        elif isinstance(self._in_queue, list):
            results = [queue.get() for queue in self._in_queue]

            if len(set([len(batch) for batch in results])) > 1:
                return Batch(status=BatchStatus.ERROR)

            if all(batch.is_end() for batch in results):
                status = BatchStatus.END
            elif all(batch.is_ok() for batch in results):
                status = BatchStatus.OK
            else:
                status = BatchStatus.ERROR

            #  TODO: fix
            meta = results[0].meta.copy()
            for batch in results:
                meta.update(batch.meta)

            batch = Batch(
                data=[
                    {
                        queue.name: batch[idx]
                        for queue, batch in zip(self._in_queue, results)
                    }
                    for idx in range(len(results[0]))
                ],
                status=status,
                meta=meta
            )
            return batch
        else:
            return self._in_queue.get()

    def put(
            self,
            batch: Optional[Batch]
    ) -> None:
        if self._out_queue is None:
            return
        elif isinstance(self._out_queue, list):
            for queue in self._out_queue:
                queue.put(batch)
        else:
            self._out_queue.put(batch)

    def _step(
            self,
            input: Batch
    ) -> StepVerdict:
        try:
            out: Batch = self.work_func() if input is None else self.work_func(input)
        except Exception as e:
            out = Batch(status=BatchStatus.ERROR, error=str(e))
            logging.error(f'During work function of node {self._name} happend error: {e}')
        if out.is_end():
            logging.info(f'Node {self._name} got END batch. Will be terminated with success behaviour.')
            self._status.value = NodeStatus.FINISH.value
            self.put(out)
            return StepVerdict.STOP
        if out.is_ok():
            self.put(out)
            return StepVerdict.CONTINUE
        # Below out.status == BatchStatus.ERROR
        self._num_retries += 1
        if self._num_retries <= self._max_retries:
            logging.warning(
                f'Node {self._name} got error status, but will try handling. Attempt {self._num_retries} from {self._max_retries}.'
            )
            return self._step(input)
        self._num_restart_retries += 1
        if self._num_restart_retries <= self._max_restart_retries:
            logging.warning(
                f'Node {self._name} got max errors for retry politic, but will be restarted. Attempt {self._num_restart_retries} from {self._max_restart_retries}.'
            )
            self._num_retries = 0
            self.close()
            self.post_init()
            return self._step(input)

        logging.warning(
            f'Node {self._name} got errors more times than the limit. This node will be terminate.'
        )
        self._status.value = NodeStatus.TERMINATE.value
        return StepVerdict.STOP

    def run(
            self
    ):
        while True:
            sleep(self._timeout)
            if not self.is_alive():
                break
            input = self.get()
            if input is not None:
                if input.is_end():
                    self._status.value = NodeStatus.FINISH.value
                    self.put(batch=input)
                    break
                elif input.is_error():
                    self._status.value = NodeStatus.TERMINATE.value
                    break

            verdict = self._step(input=input)
            if verdict == StepVerdict.CONTINUE:
                continue
            else:
                self._status.value = NodeStatus.FINISH.value if self.is_alive() else self._status.value
                break

    @property
    def status(
            self
    ) -> NodeStatus:
        return NodeStatus(self._status.value)

    @property
    def name(
            self
    ) -> str:
        return self._name

    def finish(
            self
    ) -> None:
        self._status.value = NodeStatus.FINISH.value
        if self._in_queue is None:
            return
        if isinstance(self._in_queue, list):
            for queue in self._in_queue:
                queue.put(Batch(status=BatchStatus.END))
        else:
            self._in_queue.put(Batch(status=BatchStatus.END))

    def terminate(
            self
    ):
        self._status.value = NodeStatus.TERMINATE.value
        if self._in_queue is None:
            return
        if isinstance(self._in_queue, list):
            for queue in self._in_queue:
                queue.put(Batch(status=BatchStatus.END))
        else:
            self._in_queue.put(Batch(status=BatchStatus.END))

    def is_process(self) -> bool:
        return self._type == NodeType.PROCESS

    def is_thread(self) -> bool:
        return self._type == NodeType.THREAD

    def is_alive(self) -> bool:
        return self.status == NodeStatus.ALIVE
