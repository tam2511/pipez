from enum import Enum, auto


class NodeType(Enum):
    THREAD = auto()
    PROCESS = auto()

    @staticmethod
    def from_string(type: str) -> 'NodeType':
        if type.lower() == 'thread':
            return NodeType.THREAD
        elif type.lower() == 'process':
            return NodeType.PROCESS
        else:
            raise ValueError


class NodeStatus(Enum):
    ALIVE = auto()
    FINISH = auto()
    TERMINATE = auto()


class BatchStatus(Enum):
    OK = auto()
    END = auto()
    ERROR = auto()
