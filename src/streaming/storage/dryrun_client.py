"""Cliente fake - simula gravação sem fazer I/O.

Implementa o mesmo contrato (StorageClient) que MinIOClient, permitindo
ao consumer ser executado em modo `--dry-run` sem tocar em MinIO/S3.

Uso típico: desenvolvimento local, testes de integração, validação
de pipelines sem dependência externa.
"""
import json
import logging
from typing import Any, Dict, List

log = logging.getLogger("bronze_consumer")


class DryRunClient:
    """StorageClient que apenas loga o que seria gravado."""

    def put_json(self, key: str, events: List[Dict[str, Any]]) -> int:
        body_bytes = "\n".join(
            json.dumps(e, ensure_ascii=False) for e in events
        ).encode("utf-8")

        log.info(
            "dry-run: gravação simulada",
            extra={
                "key":    key,
                "events": len(events),
                "bytes":  len(body_bytes),
                "sample": events[0].get("sensor_id") if events else None,
            }
        )
        return len(body_bytes)