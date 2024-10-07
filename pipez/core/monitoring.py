from typing import Optional
import logging
import signal
import time
import os

from pipez.core.batch import Batch
from pipez.core.enums import NodeType
from pipez.core.node import Node


class Monitoring(Node):
    def __init__(self, **kwargs):
        super().__init__(name=self.__class__.__name__, type=NodeType.PROCESS, timeout=10.0, **kwargs)

    def processing(self, data: Optional[Batch]) -> Optional[Batch]:
        if time.time() - self.shared_memory['time'] >= 60.0:
            logging.info(f'{self.name}: os.kill(1, signal.SIGTERM)')
            os.kill(1, signal.SIGTERM)
