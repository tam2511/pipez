from abc import abstractmethod, ABC
from enum import Enum, auto
from typing import Optional, Union, List
from multiprocessing import Process, Value
from threading import Thread
from time import sleep, monotonic
import logging

from pipez.core.batch import Batch, BatchStatus
from pipez.core.metrics import Metrics
from pipez.core.shared_memory import SharedMemory


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
            input: Optional[Union[str, List[str]]] = None,
            output: Optional[Union[str, List[str]]] = None,
            collector_flag: Optional[str] = None,
            **kwargs
    ) -> None:
        self._kwargs = kwargs

        self._name = name
        self._type = type
        self._max_retries = max_retries
        self._max_restart_retries = max_restart_retries
        self._timeout = timeout
        self._input = input
        self._output = output
        self._collector_flag = collector_flag

        self._num_retries = 0
        self._num_restart_retries = 0
        self._status = Value('i', NodeStatus.ALIVE.value)
        self._in_queue = None
        self._out_queue = None
        self._metrics = Metrics()
        self._memory = SharedMemory()
        self._collection = Batch()

        self._init_worker()

    @property
    def memory(self):
        return self._memory

    @property
    def metrics(self) -> Metrics:
        return self._metrics

    @property
    def input(self) -> Optional[Union[str, List[str]]]:
        return self._input

    @property
    def output(self) -> Optional[Union[str, List[str]]]:
        return self._output

    @property
    def in_queue(self):
        return self._in_queue

    @in_queue.setter
    def in_queue(self, value):
        self._in_queue = value

    @property
    def out_queue(self):
        return self._out_queue

    @out_queue.setter
    def out_queue(self, value):
        self._out_queue = value

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
        logging.debug(f'{self.name} START get') if self.name != 'WatchDog' else ...
        if self._in_queue is None:
            logging.debug(f'{self.name} END get [return None]') if self.name != 'WatchDog' else ...
            return None
        elif isinstance(self._in_queue, list):
            results = [queue.get() for queue in self._in_queue]

            if len(set([len(batch) for batch in results])) > 1:
                logging.debug(f'{self.name} END get [return BatchStatus.ERROR]') if self.name != 'WatchDog' else ...
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
            logging.debug(f'{self.name} END get [processed queue-LIST, return batch]') if self.name != 'WatchDog' else ...
            return batch
        else:
            logging.debug(f'{self.name} END get [processed queue-NOT-LIST, return batch]') if self.name != 'WatchDog' else ...
            return self._in_queue.get()

    def put(
            self,
            batch: Optional[Batch]
    ) -> None:
        logging.debug(f'{self.name} START put') if self.name != 'WatchDog' else ...
        if self._out_queue is None:
            logging.debug(f'{self.name} END put [return None]') if self.name != 'WatchDog' else ...
            return
        elif isinstance(self._out_queue, list):
            for queue in self._out_queue:
                queue.put(batch)
            logging.debug(f'{self.name} END put [processed queue-LIST]') if self.name != 'WatchDog' else ...
        else:
            self._out_queue.put(batch)
            logging.debug(f'{self.name} END put [processed queue-NOT-LIST]') if self.name != 'WatchDog' else ...

    def _step(
            self,
            input: Batch
    ) -> StepVerdict:
        try:
            st = monotonic()
            logging.debug(f'{self.name} START work_func [input is {input.__class__}]') if self.name != 'WatchDog' else ...
            out: Batch = self.work_func() if input is None else self.work_func(input)
            logging.debug(f'{self.name} END work_func [out is {out.__class__}]') if self.name != 'WatchDog' else ...
            self._metrics.update('duration', monotonic() - st)
            self._metrics.update('handled', len(out) if input is None else len(input))
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
        if out.is_skip():
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
            if self._collector_flag is not None:
                if input is None:
                    continue
                if self._collector_flag not in input.meta:
                    logging.error(
                        f'Node {self._name} have collector_flag = {self._collector_flag}, but input batch hasn\'t key.'
                    )
                    self._status.value = NodeStatus.TERMINATE.value
                    logging.info(
                        f'Node {self._name} finish loop.'
                    )
                    break
                if input.meta[self._collector_flag]:
                    verdict = self._step(input=self._collection)
                    self._collection = Batch()
                else:
                    self._collection.extend(input)
                    verdict = StepVerdict.CONTINUE
            else:
                if input is not None:
                    if input.is_end():
                        self._status.value = NodeStatus.FINISH.value
                        self.put(batch=input)
                        logging.info(
                            f'Node {self._name} finish loop.'
                        )
                        break
                    elif input.is_error():
                        self._status.value = NodeStatus.TERMINATE.value
                        logging.info(
                            f'Node {self._name} finish loop.'
                        )
                        break
                    elif input.is_skip():
                        sleep(self._timeout + 1e-2)
                        continue
                verdict = self._step(input=input)

            if verdict == StepVerdict.CONTINUE:
                continue
            else:
                self._status.value = NodeStatus.FINISH.value if self.is_alive() else self._status.value
                logging.info(
                    f'Node {self._name} finish loop.'
                )
                break

    @property
    def status(
            self
    ) -> NodeStatus:
        return NodeStatus(self._status.value)

    @property
    def name(self) -> str:
        return self._name

    def _terminate(self):
        if self._in_queue is None:
            return
        if isinstance(self._in_queue, list):
            for queue in self._in_queue:
                while queue.size():
                    queue.get()
                queue.put(Batch(status=BatchStatus.END))
        else:
            while self._in_queue.size():
                self._in_queue.get()
            self._in_queue.put(Batch(status=BatchStatus.END))

    def finish(self) -> None:
        self._status.value = NodeStatus.FINISH.value
        self._terminate()

    def terminate(self):
        self._status.value = NodeStatus.TERMINATE.value
        self._terminate()

    def is_process(self) -> bool:
        return self._type == NodeType.PROCESS

    def is_thread(self) -> bool:
        return self._type == NodeType.THREAD

    def is_alive(self) -> bool:
        return self.status == NodeStatus.ALIVE
