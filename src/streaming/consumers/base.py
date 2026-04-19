from abc import ABC, abstractmethod
from typing import Any, Iterator


class MessageConsumer(ABC):

    @abstractmethod
    def consume(self) -> Iterator[Any]:
        """Retorna um iterador de mensagens"""
        pass

    @abstractmethod
    def commit(self) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass