"""Cliente para o Confluent Schema Registry."""
import json
import logging
from typing import Any, Dict, Optional

try:
    import requests
except ImportError as exc:
    raise ImportError("requests não instalado. Execute: pip install requests") from exc

log = logging.getLogger("bronze_consumer")


class SchemaRegistryClient:
    """
    Consulta o Confluent Schema Registry e cacheia o schema do tópico
    para validação local. Falhas de conexão não levantam exceção:
    o cliente apenas marca-se como indisponível.
    """

    def __init__(self, url: str, topic: str):
        self.url     = url.rstrip("/")
        self.subject = f"{topic}-value"
        self._schema: Optional[Dict[str, Any]] = None
        self._available = False
        self._connect()

    def _connect(self) -> None:
        """Tenta conectar ao Schema Registry e baixar o schema mais recente."""
        try:
            resp = requests.get(
                f"{self.url}/subjects/{self.subject}/versions/latest",
                timeout=5,
            )
            if resp.status_code == 200:
                data = resp.json()
                self._schema = json.loads(data.get("schema", "{}"))
                self._available = True
                log.info(
                    "Schema Registry conectado",
                    extra={
                        "schema_registry_url": self.url,
                        "subject":             self.subject,
                        "schema_version":      data.get("version"),
                        "schema_id":           data.get("id"),
                    }
                )
            else:
                log.warning(
                    "Schema Registry retornou status inesperado - usando validação local",
                    extra={"status_code": resp.status_code, "subject": self.subject}
                )
        except requests.exceptions.ConnectionError:
            log.warning(
                "Schema Registry indisponível - usando validação local como fallback",
                extra={"schema_registry_url": self.url}
            )
        except Exception as exc:
            log.warning(
                "Erro ao conectar ao Schema Registry",
                extra={"error": str(exc)}
            )

    @property
    def is_available(self) -> bool:
        return self._available

    def get_required_fields(self) -> set:
        """Retorna o conjunto de campos obrigatórios definidos no schema."""
        if self._schema:
            return set(self._schema.get("required", []))
        return set()

    def get_enum_values(self, field: str) -> Optional[set]:
        """Retorna os valores válidos de um campo enum, ou None."""
        if not self._schema:
            return None
        props = self._schema.get("properties", {})
        field_schema = props.get(field, {})
        enum_values = field_schema.get("enum")
        return set(enum_values) if enum_values else None