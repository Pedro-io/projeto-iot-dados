"""Interface comum para clientes de storage (Strategy Pattern).

Define o contrato que MinIOClient, DryRunClient e qualquer backend futuro
(S3, GCS, Azure Blob...) devem implementar.
"""
from typing import Any, Dict, List, Protocol, runtime_checkable


@runtime_checkable
class StorageClient(Protocol):
    """Contrato para qualquer backend de gravação Bronze."""

    def put_json(self, key: str, events: List[Dict[str, Any]]) -> int:
        """
        Grava eventos como JSON Lines no backend.

        Args:
            key:    caminho/identificador do objeto.
            events: lista de eventos (dicts) a serializar como JSONL.

        Returns:
            Número de bytes gravados.
        """
        ...