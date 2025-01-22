from enum import Enum, auto


class NodeType(Enum):
    THREAD = auto()
    PROCESS = auto()


class NodeStatus(Enum):
    PENDING = auto()
    ALIVE = auto()
    COMPLETED = auto()
    TERMINATED = auto()


class BatchStatus(Enum):
    OK = auto()
    LAST = auto()
