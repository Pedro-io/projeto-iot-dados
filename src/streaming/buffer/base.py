from abc import ABC, abstractmethod
from typing import Dict, List


class Buffer(ABC):

    @abstractmethod
    def add(self, event: dict) -> None:
        pass

    @abstractmethod
    def should_flush(self) -> bool:
        pass

    @abstractmethod
    def drain(self) -> Dict[str, List[dict]]:
        pass