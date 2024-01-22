from typing import List, Dict, Optional, Union
from queue import Queue as tQueue
from multiprocessing import Queue as mQueue

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
            node['type'] = NodeType.from_string(node['type'])
            del node['cls']
            node = cls(
                **node
            )
            _pipeline.append(node)
        else:
            raise BrokenPipeError('Available only Node and Dict type for pipeline describing')
    return _pipeline


def build_pipeline(
        pipeline: List[Union[Dict, Node]]
) -> Node:
    pipeline = validate_pipeline(pipeline=pipeline)
    queues = dict()
    for node in pipeline:
        in_queue, out_queue = node.input, node.output
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
        queues[queue] = QueueWrapper(name=queue, queue=queues.get(queue)())

    nodes = []
    for node in pipeline:
        node.in_queue = parse_queue(queues=queues, queue_info=node.input)
        node.out_queue = parse_queue(queues=queues, queue_info=node.output)
        node.post_init()
        node.start()
        nodes.append(node)

    watchdog = WatchDog(nodes=nodes)
    watchdog.post_init()
    watchdog.start()
    return watchdog
