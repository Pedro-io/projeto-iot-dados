from typing import Any, Dict, Optional

from .base import EventValidator


class SchemaValidator(EventValidator):
    """
    Valida eventos contra o schema do Schema Registry (quando disponível)
    ou contra o schema local como fallback.
    """
 
    def __init__(self, registry_client: Optional[SchemaRegistryClient] = None):
        self.registry = registry_client
        source = "Schema Registry" if (registry_client and registry_client.is_available) else "local"
        log.info("Validação de schema inicializada", extra={"source": source})
 
    def validate(self, event: Dict[str, Any]) -> tuple:
        """Retorna (válido: bool, motivo_erro: str | None)."""
 
        # Campos obrigatórios — usa Schema Registry se disponível
        if self.registry and self.registry.is_available:
            required = self.registry.get_required_fields()
        else:
            required = REQUIRED_FIELDS
 
        missing = required - event.keys()
        if missing:
            return False, f"Campos ausentes: {sorted(missing)}"
 
        # Tipo numérico do value
        if not isinstance(event.get("value"), (int, float)):
            return False, f"'value' deve ser numérico, recebeu: {type(event['value']).__name__}"
 
        # Tipo booleano do is_anomaly
        if not isinstance(event.get("is_anomaly"), bool):
            return False, f"'is_anomaly' deve ser bool, recebeu: {type(event['is_anomaly']).__name__}"
 
        # Enum quality
        valid_qualities = (
            self.registry.get_enum_values("quality")
            if (self.registry and self.registry.is_available)
            else VALID_QUALITIES
        )
        if valid_qualities and event.get("quality") not in valid_qualities:
            return False, f"'quality' inválido: {event.get('quality')} (esperado: {valid_qualities})"
 
        # Enum measurement_type
        valid_types = (
            self.registry.get_enum_values("measurement_type")
            if (self.registry and self.registry.is_available)
            else VALID_MEASUREMENTS
        )
        if valid_types and event.get("measurement_type") not in valid_types:
            return False, f"'measurement_type' inválido: {event.get('measurement_type')}"
 
        # metadata deve ser dict
        if not isinstance(event.get("metadata"), dict):
            return False, "'metadata' deve ser um objeto JSON"
 
        return True, None
  