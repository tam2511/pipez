from typing import List, Dict

from pipez.core.node import Node
from pipez.core.registry import Registry
from pipez.core.enums import NodeType


def json2nodes(
        pipeline: List[Dict]
) -> List[Node]:
    """
    Перевод узлов из JSON-формата в Python объекты
    """
    registry = Registry()
    nodes = []

    for node in pipeline:
        if not isinstance(node, Dict):
            raise TypeError('JSON format must be class «dict»')

        cls = registry[node.pop('cls')]
        node['type'] = NodeType.from_string(node.get('type', 'thread'))
        nodes.append(cls(**node))

    return nodes
