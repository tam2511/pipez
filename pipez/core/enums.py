from enum import Enum, auto


class NodeType(Enum):
    THREAD = auto()
    PROCESS = auto()


class NodeStatus(Enum):
    PENDING = auto()
    ACTIVE = auto()
    COMPLETED = auto()
    TERMINATED = auto()


class BatchStatus(Enum):
    OK = auto()
    LAST = auto()
    ERROR = auto()
