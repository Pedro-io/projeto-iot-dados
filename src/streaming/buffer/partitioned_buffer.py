import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List

from .base import Buffer


class PartitionedBuffer(Buffer):
    """
    Agrupa eventos por (factory_id, measurement_type, data UTC) antes de
    gravar no MinIO — produz particionamento Hive-style para o Spark ler.
    """
 
    def __init__(self, flush_size: int, flush_interval: float):
        self.flush_size     = flush_size
        self.flush_interval = flush_interval
        self._data: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self._last_flush    = time.time()
 
    def partition_key(self, event: Dict[str, Any]) -> str:
        try:
            ts = datetime.fromisoformat(event["timestamp"])
        except (ValueError, KeyError):
            ts = datetime.now(timezone.utc)
        return (
            f"factory_id={event.get('factory_id', 'UNKNOWN')}"
            f"/measurement_type={event.get('measurement_type', 'UNKNOWN')}"
            f"/dt={ts.strftime('%Y-%m-%d')}"
        )
 
    def add(self, event: Dict[str, Any]) -> None:
        self._data[self.partition_key(event)].append(event)
 
    def should_flush(self) -> bool:
        if any(len(v) >= self.flush_size for v in self._data.values()):
            return True
        return time.time() - self._last_flush >= self.flush_interval
 
    def drain(self) -> Dict[str, List[Dict[str, Any]]]:
        snapshot = dict(self._data)
        self._data = defaultdict(list)
        self._last_flush = time.time()
        return snapshot
 
    def total(self) -> int:
        return sum(len(v) for v in self._data.values())