from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Tuple


class EventValidator(ABC):

    @abstractmethod
    def validate(self, event: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        pass