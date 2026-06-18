"""Buffer particionado Hive-style."""
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List


class PartitionedBuffer:
    """
    Agrupa eventos por (factory_id, measurement_type, data UTC) antes de
    persistir - produz layout Hive-style que Spark/Athena/Trino conseguem
    fazer pruning de partições.
    """

    def __init__(self, flush_size: int, flush_interval: float):
        self.flush_size     = flush_size
        self.flush_interval = flush_interval
        self._data: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self._last_flush    = time.time()

    def partition_key(self, event: Dict[str, Any]) -> str:
        """Constrói o caminho de partição (Hive-style)."""
        try:
            ts = datetime.fromisoformat(event["timestamp"])
        except (ValueError, KeyError, TypeError):
            ts = datetime.now(timezone.utc)
        return (
            f"factory_id={event.get('factory_id', 'UNKNOWN')}"
            f"/measurement_type={event.get('measurement_type', 'UNKNOWN')}"
            f"/dt={ts.strftime('%Y-%m-%d')}"
        )

    def add(self, event: Dict[str, Any]) -> None:
        self._data[self.partition_key(event)].append(event)

    def should_flush(self) -> bool:
        """Flush se: alguma partição atingiu o tamanho-alvo OU passou o intervalo."""
        if any(len(v) >= self.flush_size for v in self._data.values()):
            return True
        return time.time() - self._last_flush >= self.flush_interval

    def drain(self) -> Dict[str, List[Dict[str, Any]]]:
        """Retorna todo o conteúdo e zera o buffer."""
        snapshot = dict(self._data)
        self._data = defaultdict(list)
        self._last_flush = time.time()
        return snapshot

    def total(self) -> int:
        return sum(len(v) for v in self._data.values())