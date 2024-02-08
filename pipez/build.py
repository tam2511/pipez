from typing import List, Dict, Optional, Union
from queue import Queue as tQueue
from multiprocessing import Queue as mQueue
import logging

from pipez.node import Node, NodeType
from pipez.queue_wrapper import QueueWrapper
from pipez.watchdog import WatchDog
from pipez.registry import Registry


def parse_queue(
        queues: Dict[str, QueueWrapper],
        queue_info: Optional[Union[str, List[str]]]
) -> Optional[Union[QueueWrapper, List[QueueWrapper]]]:
    if queue_info is None:
        return None
    elif isinstance(queue_info, str):
        return queues.get(queue_info)
    else:
        return [queues[queue] for queue in queue_info]


def validate_pipeline(
        pipeline: List[Union[Dict, Node]]
) -> List[Node]:
    registry = Registry.get_instance()
    _pipeline = []
    for node in pipeline:
        if isinstance(node, Node):
            _pipeline.append(node)
        elif isinstance(node, Dict):
            cls = registry[node.get('cls')]
            node['type'] = NodeType.from_string(node.get('type', 'Thread'))
            del node['cls']
            node = cls(
                **node
            )
            _pipeline.append(node)
        else:
            raise BrokenPipeError('Available only Node and Dict type for pipeline describing')
    return _pipeline


def build_pipeline(
        pipeline: List[Union[Dict, Node]],
        verbose_metrics: bool = False,
        metrics_host: str = '127.0.0.1',
        metrics_port: int = 8887,
        debug: bool = False,
        path_log: str = 'log.log'
) -> Node:
    if debug:
        logging.basicConfig(filename=path_log, level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")
    pipeline = validate_pipeline(pipeline=pipeline)
    queues = dict()
    for node in pipeline:
        in_queue, out_queue = node.input, node.output
        if in_queue is None:
            in_queue = []
        if out_queue is None:
            out_queue = []
        if not isinstance(in_queue, list):
            in_queue = [in_queue]
        if not isinstance(out_queue, list):
            out_queue = [out_queue]
        for queue in in_queue + out_queue:
            if queue not in queues:
                queues[queue] = tQueue
            if node.is_process():
                queues[queue] = mQueue

    for queue in queues:
        queues[queue] = QueueWrapper(name=queue, queue=queues.get(queue)(maxsize=32))

    nodes = []
    for node in pipeline:
        node.in_queue = parse_queue(queues=queues, queue_info=node.input)
        node.out_queue = parse_queue(queues=queues, queue_info=node.output)
        node.post_init()
        node.start()
        nodes.append(node)

    watchdog = WatchDog(nodes=nodes,
                        verbose_metrics=verbose_metrics,
                        metrics_host=metrics_host,
                        metrics_port=metrics_port)
    watchdog.post_init()
    watchdog.start()
    return watchdog
