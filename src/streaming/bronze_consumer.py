#!/usr/bin/env python3
"""
Consumer Kafka — Camada Bronze
==============================
Consome eventos do tópico 'iot-sensors-raw' e persiste os dados brutos
no MinIO com particionamento Hive-style.
 
Novidades em relação à versão anterior:
  - Logs estruturados em JSON (compatível com ferramentas de observabilidade)
  - Validação de schema via Schema Registry (quando disponível)
  - Fallback para validação local se o Schema Registry estiver offline
 
Uso:
    python bronze_consumer.py [opções]
 
Exemplos:
    python bronze_consumer.py
    python bronze_consumer.py --schema-registry-url http://localhost:8081
    python bronze_consumer.py --dry-run
 
Requisitos:
    pip install kafka-python boto3 requests
"""
 
import argparse
import json
import logging
import signal
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
 
# ---------------------------------------------------------------------------
# Dependências obrigatórias
# ---------------------------------------------------------------------------
try:
    from kafka import KafkaConsumer
    from kafka.errors import CommitFailedError, KafkaError
except ImportError:
    print("Erro: kafka-python não instalado. Execute: pip install kafka-python")
    sys.exit(1)
 
try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:
    print("Erro: boto3 não instalado. Execute: pip install boto3")
    sys.exit(1)
 
try:
    import requests
except ImportError:
    print("Erro: requests não instalado. Execute: pip install requests")
    sys.exit(1)
 
 
# =============================================================================
# LOGGING ESTRUTURADO EM JSON
# =============================================================================
 
class JsonFormatter(logging.Formatter):
    """
    Formata logs como JSON Lines — cada linha é um objeto JSON independente.
    Compatível com Elasticsearch, Loki, CloudWatch e qualquer stack de observabilidade.
 
    Exemplo de saída:
        {"timestamp": "2025-03-29T14:30:00Z", "level": "INFO",
         "logger": "bronze_consumer", "message": "Consumer iniciado",
         "topic": "iot-sensors-raw", "group": "bronze-writer"}
    """
 
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "level":     record.levelname,
            "logger":    record.name,
            "message":   record.getMessage(),
        }
 
        # Incluir informações de exceção se houver
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
 
        # Incluir campos extras adicionados via extra={}
        for key, value in record.__dict__.items():
            if key not in (
                "name", "msg", "args", "levelname", "levelno", "pathname",
                "filename", "module", "exc_info", "exc_text", "stack_info",
                "lineno", "funcName", "created", "msecs", "relativeCreated",
                "thread", "threadName", "processName", "process", "message",
            ):
                if not key.startswith("_"):
                    log_entry[key] = value
 
        return json.dumps(log_entry, ensure_ascii=False)
 
 
def setup_logging(level: str = "INFO") -> logging.Logger:
    """Configura o sistema de logging com saída JSON estruturada."""
    logger = logging.getLogger("bronze_consumer")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
 
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
 
    # Silenciar loggers verbosos de bibliotecas externas
    logging.getLogger("kafka").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
 
    return logger
 
 
log = setup_logging()
 
 
# =============================================================================
# CONFIGURAÇÕES PADRÃO
# =============================================================================
 
DEFAULT_BOOTSTRAP_SERVERS    = "localhost:9092"
DEFAULT_TOPIC                = "iot-sensors-raw"
DEFAULT_GROUP_ID             = "bronze-writer"
DEFAULT_MINIO_ENDPOINT       = "http://localhost:9000"
DEFAULT_MINIO_ACCESS_KEY     = "minioadmin"
DEFAULT_MINIO_SECRET_KEY     = "minioadmin"
DEFAULT_BUCKET               = "bronze"
DEFAULT_FLUSH_SIZE           = 100
DEFAULT_FLUSH_INTERVAL       = 30
DEFAULT_SCHEMA_REGISTRY_URL  = "http://localhost:8081"
 
 
# =============================================================================
# SCHEMA LOCAL (fallback quando Schema Registry está offline)
# =============================================================================
 
REQUIRED_FIELDS = {
    "event_id", "sensor_id", "equipment_id", "factory_id",
    "measurement_type", "value", "unit", "timestamp", "quality",
    "is_anomaly", "metadata",
}
 
VALID_QUALITIES    = {"good", "warning", "bad"}
VALID_MEASUREMENTS = {"temperature", "humidity", "pressure", "vibration", "current"}
 
 
# =============================================================================
# SCHEMA REGISTRY CLIENT
# =============================================================================
 
class SchemaRegistryClient:
    """
    Cliente para o Confluent Schema Registry.
    Consulta e cacheia o schema do tópico para validação local.
    """
 
    def __init__(self, url: str, topic: str):
        self.url     = url.rstrip("/")
        self.subject = f"{topic}-value"
        self._schema: Optional[Dict[str, Any]] = None
        self._available = False
        self._connect()
 
    def _connect(self) -> None:
        """Tenta conectar ao Schema Registry e baixar o schema."""
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
                        "subject": self.subject,
                        "schema_version": data.get("version"),
                        "schema_id": data.get("id"),
                    }
                )
            else:
                log.warning(
                    "Schema Registry retornou status inesperado — usando validação local",
                    extra={"status_code": resp.status_code, "subject": self.subject}
                )
        except requests.exceptions.ConnectionError:
            log.warning(
                "Schema Registry indisponível — usando validação local como fallback",
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
        """Extrai campos obrigatórios do schema registrado."""
        if self._schema:
            return set(self._schema.get("required", []))
        return REQUIRED_FIELDS
 
    def get_enum_values(self, field: str) -> Optional[set]:
        """Extrai valores válidos de um campo enum no schema."""
        if not self._schema:
            return None
        props = self._schema.get("properties", {})
        field_schema = props.get(field, {})
        enum_values = field_schema.get("enum")
        return set(enum_values) if enum_values else None
 
 
# =============================================================================
# VALIDAÇÃO DE SCHEMA
# =============================================================================
 
class SchemaValidator:
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
 
 
# =============================================================================
# CLIENTE MINIO
# =============================================================================
 
class MinIOClient:
    """Wrapper sobre boto3 para gravar arquivos JSON Lines no MinIO."""
 
    def __init__(self, endpoint: str, access_key: str, secret_key: str, bucket: str):
        self.bucket = bucket
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=boto3.session.Config(signature_version="s3v4"),
        )
        self._ensure_bucket()
 
    def _ensure_bucket(self) -> None:
        try:
            self.client.head_bucket(Bucket=self.bucket)
            log.info("Bucket verificado", extra={"bucket": self.bucket, "status": "exists"})
        except ClientError:
            self.client.create_bucket(Bucket=self.bucket)
            log.info("Bucket criado", extra={"bucket": self.bucket, "status": "created"})
 
    def put_json(self, key: str, events: List[Dict[str, Any]]) -> int:
        """Grava eventos como JSON Lines. Retorna bytes gravados."""
        body = "\n".join(json.dumps(e, ensure_ascii=False) for e in events)
        body_bytes = body.encode("utf-8")
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body_bytes,
            ContentType="application/x-ndjson",
        )
        return len(body_bytes)
 
 
# =============================================================================
# BUFFER PARTICIONADO
# =============================================================================
 
class PartitionedBuffer:
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
 
 
# =============================================================================
# CONSUMER PRINCIPAL
# =============================================================================
 
class BronzeConsumer:
    """
    Consome 'iot-sensors-raw' e persiste eventos brutos na camada Bronze.
    Usa commit manual de offset para garantir at-least-once delivery.
    """
 
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        minio_client: Optional[MinIOClient],
        schema_registry_url: str,
        flush_size: int     = DEFAULT_FLUSH_SIZE,
        flush_interval: int = DEFAULT_FLUSH_INTERVAL,
        dry_run: bool       = False,
    ):
        self.topic   = topic
        self.dry_run = dry_run
        self.minio   = minio_client
        self.buffer  = PartitionedBuffer(flush_size, flush_interval)
 
        # Schema Registry + validador
        registry = SchemaRegistryClient(schema_registry_url, topic)
        self.validator = SchemaValidator(registry)
 
        # Contadores
        self._total_received  = 0
        self._total_written   = 0
        self._total_anomalies = 0
        self._total_invalid   = 0
        self._total_bytes     = 0
        self._session_start   = time.time()
 
        # Dead-letter buffer
        self._dlq: List[Dict[str, Any]] = []
 
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            max_partition_fetch_bytes=1_048_576,
            session_timeout_ms=30_000,
            heartbeat_interval_ms=10_000,
        )
 
        log.info(
            "Consumer iniciado",
            extra={
                "topic": topic,
                "group_id": group_id,
                "bootstrap_servers": bootstrap_servers,
                "dry_run": dry_run,
                "flush_size": flush_size,
                "flush_interval_s": flush_interval,
                "schema_registry": schema_registry_url,
            }
        )
 
    # ------------------------------------------------------------------
    # GRAVAÇÃO NO MINIO
    # ------------------------------------------------------------------
 
    def _build_object_key(self, partition_path: str) -> str:
        ts_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        return f"{partition_path}/batch_{ts_str}.ndjson"
 
    def _flush_to_minio(self) -> None:
        snapshot = self.buffer.drain()
        if not snapshot:
            return
 
        for partition_path, events in snapshot.items():
            if not events:
                continue
 
            key = self._build_object_key(partition_path)
 
            if self.dry_run:
                log.info(
                    "dry-run: gravação simulada",
                    extra={
                        "partition": partition_path,
                        "events": len(events),
                        "key": key,
                        "sample": events[0].get("sensor_id"),
                    }
                )
                self._total_written += len(events)
                continue
 
            try:
                bytes_written = self.minio.put_json(key, events)
                self._total_written += len(events)
                self._total_bytes   += bytes_written
                log.info(
                    "Bronze gravado",
                    extra={
                        "key": key,
                        "events": len(events),
                        "bytes": bytes_written,
                        "kb": round(bytes_written / 1024, 1),
                    }
                )
            except (BotoCoreError, ClientError) as exc:
                log.error(
                    "Falha ao gravar no MinIO",
                    extra={"key": key, "error": str(exc)}
                )
                raise
 
        if self._dlq:
            self._flush_dlq()
 
    def _flush_dlq(self) -> None:
        ts_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        key    = f"_dead_letter/dt={datetime.utcnow():%Y-%m-%d}/invalid_{ts_str}.ndjson"
 
        if self.dry_run:
            log.warning(
                "dry-run: DLQ simulada",
                extra={"events": len(self._dlq), "key": key}
            )
            self._dlq = []
            return
 
        try:
            self.minio.put_json(key, self._dlq)
            log.warning(
                "DLQ gravada",
                extra={"events": len(self._dlq), "key": key}
            )
            self._dlq = []
        except Exception as exc:
            log.error("Falha ao gravar DLQ", extra={"error": str(exc)})
 
    # ------------------------------------------------------------------
    # PROCESSAMENTO
    # ------------------------------------------------------------------
 
    def _process_message(self, message) -> None:
        self._total_received += 1
        event: Dict[str, Any] = message.value
 
        valid, reason = self.validator.validate(event)
        if not valid:
            log.warning(
                "Evento inválido — enviado para DLQ",
                extra={
                    "offset": message.offset,
                    "partition": message.partition,
                    "kafka_key": message.key,
                    "reason": reason,
                }
            )
            event["_dlq_reason"]      = reason
            event["_kafka_offset"]    = message.offset
            event["_kafka_partition"] = message.partition
            self._dlq.append(event)
            self._total_invalid += 1
            return
 
        # Enriquecer com metadados de ingestão
        event["_ingested_at"]       = datetime.utcnow().isoformat() + "Z"
        event["_kafka_offset"]      = message.offset
        event["_kafka_partition"]   = message.partition
        event["_kafka_topic"]       = message.topic
        event["_kafka_key"]         = message.key
 
        if event.get("is_anomaly"):
            self._total_anomalies += 1
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
 
        self.buffer.add(event)
 
    # ------------------------------------------------------------------
    # LOOP PRINCIPAL
    # ------------------------------------------------------------------
 
    def run(self) -> None:
        log.info("Aguardando mensagens", extra={"topic": self.topic})
 
        try:
            for message in self.consumer:
                try:
                    self._process_message(message)
                except Exception as exc:
                    log.error(
                        "Erro ao processar mensagem",
                        extra={"offset": message.offset, "error": str(exc)}
                    )
 
                if self.buffer.should_flush():
                    try:
                        self._flush_to_minio()
                        self.consumer.commit()
                        self._log_stats()
                    except Exception as exc:
                        log.error(
                            "Flush falhou — offset NÃO commitado",
                            extra={"error": str(exc)}
                        )
 
        except KeyboardInterrupt:
            log.info("Interrompido pelo usuário")
        except CommitFailedError as exc:
            log.error("Falha no commit de offset", extra={"error": str(exc)})
        finally:
            self._shutdown()
 
    def _shutdown(self) -> None:
        log.info("Encerrando — gravando buffer restante")
        remaining = self.buffer.total()
        if remaining > 0:
            try:
                self._flush_to_minio()
                self.consumer.commit()
                log.info("Buffer final commitado", extra={"events": remaining})
            except Exception as exc:
                log.error("Falha no flush final", extra={"error": str(exc)})
 
        self.consumer.close()
        self._log_final_stats()
 
    # ------------------------------------------------------------------
    # ESTATÍSTICAS — saída em JSON estruturado
    # ------------------------------------------------------------------
 
    def _log_stats(self) -> None:
        elapsed = time.time() - self._session_start
        log.info(
            "Estatísticas parciais",
            extra={
                "received":   self._total_received,
                "written":    self._total_written,
                "anomalies":  self._total_anomalies,
                "invalid":    self._total_invalid,
                "total_kb":   round(self._total_bytes / 1024, 1),
                "elapsed_s":  round(elapsed, 1),
                "rate_per_s": round(self._total_received / elapsed, 1) if elapsed else 0,
            }
        )
 
    def _log_final_stats(self) -> None:
        elapsed = time.time() - self._session_start
        log.info(
            "Sessão encerrada",
            extra={
                "received":   self._total_received,
                "written":    self._total_written,
                "anomalies":  self._total_anomalies,
                "invalid":    self._total_invalid,
                "total_mb":   round(self._total_bytes / 1_048_576, 2),
                "elapsed_s":  round(elapsed, 1),
                "rate_per_s": round(self._total_received / elapsed, 1) if elapsed else 0,
            }
        )
 
 
# =============================================================================
# MAIN
# =============================================================================
 
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Consumer Kafka → Bronze (MinIO) com Schema Registry.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
 
    parser.add_argument("--bootstrap-servers",   default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic",               default=DEFAULT_TOPIC)
    parser.add_argument("--group-id",            default=DEFAULT_GROUP_ID)
    parser.add_argument("--minio-endpoint",      default=DEFAULT_MINIO_ENDPOINT)
    parser.add_argument("--minio-access-key",    default=DEFAULT_MINIO_ACCESS_KEY)
    parser.add_argument("--minio-secret-key",    default=DEFAULT_MINIO_SECRET_KEY)
    parser.add_argument("--bucket",              default=DEFAULT_BUCKET)
    parser.add_argument("--flush-size",          type=int, default=DEFAULT_FLUSH_SIZE)
    parser.add_argument("--flush-interval",      type=int, default=DEFAULT_FLUSH_INTERVAL)
    parser.add_argument("--schema-registry-url", default=DEFAULT_SCHEMA_REGISTRY_URL,
                        help="URL do Confluent Schema Registry")
    parser.add_argument("--dry-run",             action="store_true")
    parser.add_argument("--log-level",           default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
 
    args = parser.parse_args()
 
    # Reconfigurar log level se necessário
    log.setLevel(getattr(logging, args.log_level.upper()))
 
    log.info(
        "Iniciando Consumer Kafka → Bronze",
        extra={
            "topic":               args.topic,
            "group_id":            args.group_id,
            "bootstrap_servers":   args.bootstrap_servers,
            "minio_endpoint":      args.minio_endpoint,
            "bucket":              args.bucket,
            "flush_size":          args.flush_size,
            "flush_interval":      args.flush_interval,
            "schema_registry_url": args.schema_registry_url,
            "dry_run":             args.dry_run,
        }
    )
 
    # Instanciar MinIO
    minio_client: Optional[MinIOClient] = None
    if not args.dry_run:
        try:
            minio_client = MinIOClient(
                endpoint   = args.minio_endpoint,
                access_key = args.minio_access_key,
                secret_key = args.minio_secret_key,
                bucket     = args.bucket,
            )
        except Exception as exc:
            log.error("Falha ao conectar ao MinIO", extra={"error": str(exc)})
            sys.exit(1)
 
    # Instanciar e rodar consumer
    consumer = BronzeConsumer(
        bootstrap_servers   = args.bootstrap_servers,
        topic               = args.topic,
        group_id            = args.group_id,
        minio_client        = minio_client,
        schema_registry_url = args.schema_registry_url,
        flush_size          = args.flush_size,
        flush_interval      = args.flush_interval,
        dry_run             = args.dry_run,
    )
 
    # Capturar SIGTERM (Docker stop)
    def _handle_sigterm(signum, frame):
        log.info("SIGTERM recebido — encerrando graciosamente")
        raise KeyboardInterrupt
 
    signal.signal(signal.SIGTERM, _handle_sigterm)
 
    consumer.run()
 
 
if __name__ == "__main__":
    main()
