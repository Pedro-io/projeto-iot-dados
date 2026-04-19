from abc import ABC, abstractmethod


class ObjectStorage(ABC):

    @abstractmethod
    def put_object(self, key: str, data: bytes) -> int:
        """Retorna quantidade de bytes escritos"""
        pass