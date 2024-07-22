from enum import Enum, auto


class NodeType(Enum):
    """
    Тип узла
    """
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
    """
    Статус узла
    """
    ALIVE = auto()
    FINISH = auto()
    TERMINATE = auto()


class BatchStatus(Enum):
    """
    Статус пакета данных
    """
    OK = auto()
    END = auto()
    ERROR = auto()
