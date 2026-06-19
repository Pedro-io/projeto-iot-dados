"""Lógica de transformação Bronze: validação + enriquecimento."""
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

from streaming.validation.schema_validator import SchemaValidator

log = logging.getLogger("bronze_consumer")


@dataclass
class ProcessResult:
    """Resultado da transformação de uma mensagem Kafka."""
    is_valid:   bool
    event:      Dict[str, Any]
    reason:     Optional[str] = None
    is_anomaly: bool = False


class BronzeProcessor:
    """
    Transforma mensagens Kafka em eventos Bronze:
      1. Valida contra schema (Registry ou local)
      2. Enriquece com metadados de ingestão (Kafka offset/partition/topic/key)
      3. Sinaliza anomalias quando is_anomaly == True

    Retorna sempre um ProcessResult - não levanta exceção para eventos
    inválidos, apenas marca is_valid=False com o motivo.
    """

    def __init__(self, validator: SchemaValidator):
        self.validator = validator

    def process(self, message: Any) -> ProcessResult:
        event: Dict[str, Any] = message.value

        valid, reason = self.validator.validate(event)
        if not valid:
            event["_dlq_reason"]      = reason
            event["_kafka_offset"]    = message.offset
            event["_kafka_partition"] = message.partition
            return ProcessResult(is_valid=False, event=event, reason=reason)

        # Enriquecimento - metadados de ingestão (prefixo "_" para distinguir
        # do payload original)
        event["_ingested_at"]     = datetime.utcnow().isoformat() + "Z"
        event["_kafka_offset"]    = message.offset
        event["_kafka_partition"] = message.partition
        event["_kafka_topic"]     = message.topic
        event["_kafka_key"]       = message.key

        is_anomaly = bool(event.get("is_anomaly"))
        if is_anomaly:
            log.warning(
                "Anomalia detectada",
                extra={
                    "sensor_id":        event.get("sensor_id"),
                    "equipment_id":     event.get("equipment_id"),
                    "factory_id":       event.get("factory_id"),
                    "measurement_type": event.get("measurement_type"),
                    "value":            event.get("value"),
                    "quality":          event.get("quality"),
                }
            )

        return ProcessResult(is_valid=True, event=event, is_anomaly=is_anomaly)