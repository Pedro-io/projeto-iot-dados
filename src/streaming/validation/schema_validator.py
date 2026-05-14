"""Validador de eventos — Schema Registry com fallback local."""
import logging
from typing import Any, Dict, Optional, Tuple

from streaming.schema.local_schema import (
    REQUIRED_FIELDS, VALID_QUALITIES, VALID_MEASUREMENTS,
)
from streaming.schema.schema_registry_client import SchemaRegistryClient

log = logging.getLogger("bronze_consumer")


class SchemaValidator:
    """
    Valida eventos contra o schema do Schema Registry (quando disponível)
    ou contra o schema local como fallback.

    Retorna sempre uma tupla (válido, motivo) — nunca levanta exceção
    para erros de validação, apenas para erros programáticos.
    """

    def __init__(self, registry_client: Optional[SchemaRegistryClient] = None):
        self.registry = registry_client
        source = "Schema Registry" if (registry_client and registry_client.is_available) else "local"
        log.info("Validação de schema inicializada", extra={"source": source})

    def _required_fields(self) -> set:
        if self.registry and self.registry.is_available:
            fields = self.registry.get_required_fields()
            if fields:
                return fields
        return set(REQUIRED_FIELDS)

    def _enum_values(self, field: str, fallback: set) -> set:
        if self.registry and self.registry.is_available:
            values = self.registry.get_enum_values(field)
            if values:
                return values
        return set(fallback)

    def validate(self, event: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Retorna (válido: bool, motivo_erro: str | None)."""
        if not isinstance(event, dict):
            return False, f"Evento deve ser dict, recebeu: {type(event).__name__}"

        missing = self._required_fields() - event.keys()
        if missing:
            return False, f"Campos ausentes: {sorted(missing)}"

        if not isinstance(event.get("value"), (int, float)):
            return False, f"'value' deve ser numérico, recebeu: {type(event.get('value')).__name__}"

        if not isinstance(event.get("is_anomaly"), bool):
            return False, f"'is_anomaly' deve ser bool, recebeu: {type(event.get('is_anomaly')).__name__}"

        valid_qualities = self._enum_values("quality", VALID_QUALITIES)
        if event.get("quality") not in valid_qualities:
            return False, f"'quality' inválido: {event.get('quality')} (esperado: {sorted(valid_qualities)})"

        valid_types = self._enum_values("measurement_type", VALID_MEASUREMENTS)
        if event.get("measurement_type") not in valid_types:
            return False, f"'measurement_type' inválido: {event.get('measurement_type')}"

        if not isinstance(event.get("metadata"), dict):
            return False, "'metadata' deve ser um objeto JSON"

        return True, None