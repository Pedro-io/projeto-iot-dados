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
# Dependências opcionais com feedback claro
# ---------------------------------------------------------------------------
try:
    from kafka import KafkaConsumer
    from kafka.errors import KafkaError, CommitFailedError
except ImportError:
    print("Erro: kafka-python não instalado. Execute: pip install kafka-python")
    sys.exit(1)

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:
    print("Erro: boto3 não instalado. Execute: pip install boto3")
    sys.exit(1)


# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("bronze_consumer")


# =============================================================================
# CONFIGURAÇÕES PADRÃO (espelham os defaults do sensor_simulator.py)
# =============================================================================

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC             = "iot-sensors-raw"       # mesmo default do simulador
DEFAULT_GROUP_ID          = "bronze-writer"          # consumer group dedicado à camada Bronze
DEFAULT_MINIO_ENDPOINT    = "http://localhost:9000"
DEFAULT_MINIO_ACCESS_KEY  = "minioadmin"
DEFAULT_MINIO_SECRET_KEY  = "minioadmin"
DEFAULT_BUCKET            = "bronze"
DEFAULT_FLUSH_SIZE        = 100    # mensagens por buffer antes de gravar
DEFAULT_FLUSH_INTERVAL    = 30     # segundos máximos entre gravações


# =============================================================================
# SCHEMA ESPERADO (referência do sensor_simulator.py)
# =============================================================================

REQUIRED_FIELDS = {
    "event_id", "sensor_id", "equipment_id", "factory_id",
    "measurement_type", "value", "unit", "timestamp", "quality",
    "is_anomaly", "metadata",
}

VALID_QUALITIES    = {"good", "warning", "bad"}
VALID_MEASUREMENTS = {"temperature", "humidity", "pressure", "vibration", "current"}


# =============================================================================
# CLIENTE MINIO
# =============================================================================

class MinIOClient:
    """Wrapper sobre boto3 para gravar arquivos JSON no MinIO."""

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str,
    ):
        self.bucket = bucket
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            # Sem SSL para ambiente local
            config=boto3.session.Config(signature_version="s3v4"),
        )
        self._ensure_bucket()

    def _ensure_bucket(self) -> None:
        """Cria o bucket Bronze se ainda não existir."""
        try:
            self.client.head_bucket(Bucket=self.bucket)
            log.info("Bucket '%s' já existe.", self.bucket)
        except ClientError:
            self.client.create_bucket(Bucket=self.bucket)
            log.info("Bucket '%s' criado.", self.bucket)

    def put_json(self, key: str, events: List[Dict[str, Any]]) -> int:
        """
        Grava lista de eventos como JSON Lines no MinIO.

        Retorna o número de bytes gravados.
        """
        # JSON Lines: um evento por linha — mais fácil de processar no Spark
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
# VALIDAÇÃO DE SCHEMA
# =============================================================================

class SchemaValidator:
    """
    Valida eventos produzidos pelo sensor_simulator.py.
    Mensagens inválidas vão para a dead-letter queue (arquivo separado no MinIO).
    """

    def validate(self, event: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """
        Retorna (válido, motivo_do_erro).
        """
        # Campos obrigatórios
        missing = REQUIRED_FIELDS - event.keys()
        if missing:
            return False, f"Campos ausentes: {missing}"

        # Tipos básicos
        if not isinstance(event.get("value"), (int, float)):
            return False, f"'value' deve ser numérico, recebeu: {type(event['value'])}"

        if not isinstance(event.get("is_anomaly"), bool):
            return False, f"'is_anomaly' deve ser bool, recebeu: {type(event['is_anomaly'])}"

        if event.get("quality") not in VALID_QUALITIES:
            return False, f"'quality' inválido: {event.get('quality')}"

        if event.get("measurement_type") not in VALID_MEASUREMENTS:
            return False, f"'measurement_type' inválido: {event.get('measurement_type')}"

        # metadata deve existir como dict
        if not isinstance(event.get("metadata"), dict):
            return False, "'metadata' deve ser um objeto"

        return True, None


# =============================================================================
# BUFFER POR PARTIÇÃO DE DESTINO
# =============================================================================

class PartitionedBuffer:
    """
    Agrupa eventos por (factory_id, measurement_type, data UTC) antes de
    gravar no MinIO — produz particionamento Hive-style para o Spark ler.

    Caminho resultante:
        bronze/
          factory_id=FAB-SP-01/
            measurement_type=temperature/
              dt=2025-03-15/
                batch_20250315_143022_abc123.ndjson
    """

    def __init__(self, flush_size: int, flush_interval: float):
        self.flush_size     = flush_size
        self.flush_interval = flush_interval
        # chave → lista de eventos
        self._data: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self._last_flush    = time.time()

    def partition_key(self, event: Dict[str, Any]) -> str:
        """Monta a chave de particionamento a partir dos campos do evento."""
        try:
            ts = datetime.fromisoformat(event["timestamp"])
        except (ValueError, KeyError):
            ts = datetime.now(timezone.utc)

        date_str         = ts.strftime("%Y-%m-%d")
        factory_id       = event.get("factory_id", "UNKNOWN")
        measurement_type = event.get("measurement_type", "UNKNOWN")

        return f"factory_id={factory_id}/measurement_type={measurement_type}/dt={date_str}"

    def add(self, event: Dict[str, Any]) -> None:
        key = self.partition_key(event)
        self._data[key].append(event)

    def should_flush(self) -> bool:
        """Flush quando alguma partição atinge o tamanho máximo OU o intervalo expira."""
        if any(len(v) >= self.flush_size for v in self._data.values()):
            return True
        if time.time() - self._last_flush >= self.flush_interval:
            return True
        return False

    def drain(self) -> Dict[str, List[Dict[str, Any]]]:
        """Retorna o conteúdo atual e limpa o buffer."""
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

    Estratégia de commit:
        enable_auto_commit=False — o offset só é confirmado APÓS a gravação
        bem-sucedida no MinIO. Se o processo cair entre o consumo e a gravação,
        as mensagens serão reprocessadas (at-least-once), evitando perda de dados.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        minio_client: Optional[MinIOClient],
        flush_size: int     = DEFAULT_FLUSH_SIZE,
        flush_interval: int = DEFAULT_FLUSH_INTERVAL,
        dry_run: bool       = False,
    ):
        self.topic      = topic
        self.dry_run    = dry_run
        self.minio      = minio_client
        self.validator  = SchemaValidator()
        self.buffer     = PartitionedBuffer(flush_size, flush_interval)

        # Contadores de sessão
        self._total_received  = 0
        self._total_written   = 0
        self._total_anomalies = 0
        self._total_invalid   = 0
        self._total_bytes     = 0

        # Dead-letter buffer (eventos inválidos)
        self._dlq: List[Dict[str, Any]] = []

        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            # Começa do início caso não haja offset salvo (primeira execução)
            auto_offset_reset="earliest",
            # Commit manual: só após gravar no MinIO
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            # Chave vem como bytes (equipment_id codificado pelo simulador)
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            # Tamanho máximo de fetch para evitar pressão de memória
            max_partition_fetch_bytes=1_048_576,  # 1 MB
            # Sessão: detectar consumidores mortos em 30s
            session_timeout_ms=30_000,
            heartbeat_interval_ms=10_000,
        )

        log.info("Consumer iniciado | tópico=%s | group=%s | dry_run=%s",
                 topic, group_id, dry_run)

    # ------------------------------------------------------------------
    # GRAVAÇÃO NO MINIO
    # ------------------------------------------------------------------

    def _build_object_key(self, partition_path: str) -> str:
        """
        Monta o caminho S3 do arquivo dentro da partição.

        Exemplo:
            factory_id=FAB-SP-01/measurement_type=temperature/dt=2025-03-15/
                batch_20250315_143022.ndjson
        """
        ts_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        return f"{partition_path}/batch_{ts_str}.ndjson"

    def _flush_to_minio(self) -> None:
        """Drena o buffer e grava cada partição como um arquivo separado."""
        snapshot = self.buffer.drain()
        if not snapshot:
            return

        for partition_path, events in snapshot.items():
            if not events:
                continue

            key = self._build_object_key(partition_path)

            if self.dry_run:
                log.info("[dry-run] Gravaria %d eventos → %s/%s",
                         len(events), DEFAULT_BUCKET, key)
                # Mostra o primeiro evento como exemplo
                log.info("[dry-run] Exemplo: %s", json.dumps(events[0], indent=2))
                self._total_written += len(events)
                continue

            try:
                bytes_written = self.minio.put_json(key, events)
                self._total_written += len(events)
                self._total_bytes   += bytes_written
                log.info("✓ Bronze gravado | key=%s | eventos=%d | %.1f KB",
                         key, len(events), bytes_written / 1024)
            except (BotoCoreError, ClientError) as exc:
                # Não abortar — logar e continuar. Os eventos ficarão no Kafka
                # (offset não commitado) e serão reprocessados.
                log.error("Falha ao gravar no MinIO: %s | key=%s", exc, key)
                raise  # Re-lança para impedir o commit do offset

        # Flush da dead-letter queue
        if self._dlq:
            self._flush_dlq()

    def _flush_dlq(self) -> None:
        """Grava eventos inválidos numa pasta separada para inspeção."""
        ts_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        key    = f"_dead_letter/dt={datetime.utcnow():%Y-%m-%d}/invalid_{ts_str}.ndjson"

        if self.dry_run:
            log.warning("[dry-run] DLQ: %d eventos inválidos seriam gravados em %s", len(self._dlq), key)
            self._dlq = []
            return

        try:
            self.minio.put_json(key, self._dlq)
            log.warning("DLQ gravada | %d eventos inválidos | key=%s", len(self._dlq), key)
            self._dlq = []
        except Exception as exc:
            log.error("Falha ao gravar DLQ: %s", exc)

    # ------------------------------------------------------------------
    # PROCESSAMENTO DE MENSAGENS
    # ------------------------------------------------------------------

    def _process_message(self, message) -> None:
        """Valida e enfileira uma mensagem Kafka."""
        self._total_received += 1
        event: Dict[str, Any] = message.value

        # Validação de schema
        valid, reason = self.validator.validate(event)
        if not valid:
            log.warning("Evento inválido (offset=%d, key=%s): %s",
                        message.offset, message.key, reason)
            # Enriquecer com contexto de ingestão antes de mandar para DLQ
            event["_dlq_reason"]    = reason
            event["_kafka_offset"]  = message.offset
            event["_kafka_partition"] = message.partition
            self._dlq.append(event)
            self._total_invalid += 1
            return

        # Enriquecer com metadados de ingestão (transparentes para o Spark)
        event["_ingested_at"]       = datetime.utcnow().isoformat() + "Z"
        event["_kafka_offset"]      = message.offset
        event["_kafka_partition"]   = message.partition
        event["_kafka_topic"]       = message.topic
        # A key no simulador é o equipment_id (útil para debug)
        event["_kafka_key"]         = message.key

        # Contar anomalias (campo is_anomaly vem do simulador)
        if event.get("is_anomaly"):
            self._total_anomalies += 1

        self.buffer.add(event)

    # ------------------------------------------------------------------
    # LOOP PRINCIPAL
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Inicia o loop de consumo. Bloqueante até SIGINT/SIGTERM."""
        log.info("Aguardando mensagens em '%s'...", self.topic)

        try:
            for message in self.consumer:
                try:
                    self._process_message(message)
                except Exception as exc:
                    log.error("Erro ao processar mensagem offset=%d: %s",
                              message.offset, exc)

                # Verificar se deve fazer flush
                if self.buffer.should_flush():
                    try:
                        self._flush_to_minio()
                        # Commit manual APÓS gravar com sucesso
                        self.consumer.commit()
                        self._print_stats()
                    except Exception as exc:
                        log.error("Flush falhou, offset NÃO commitado: %s", exc)
                        # O consumer vai reprocessar essas mensagens ao reiniciar

        except KeyboardInterrupt:
            log.info("Interrompido pelo usuário (Ctrl+C).")
        except CommitFailedError as exc:
            log.error("Falha no commit de offset: %s", exc)
        finally:
            self._shutdown()

    def _shutdown(self) -> None:
        """Grava o que está no buffer e fecha conexões."""
        log.info("Encerrando — gravando buffer restante...")
        remaining = self.buffer.total()
        if remaining > 0:
            try:
                self._flush_to_minio()
                self.consumer.commit()
                log.info("Buffer final commitado (%d eventos).", remaining)
            except Exception as exc:
                log.error("Falha no flush final: %s", exc)

        self.consumer.close()
        self._print_final_stats()

    # ------------------------------------------------------------------
    # ESTATÍSTICAS
    # ------------------------------------------------------------------

    def _print_stats(self) -> None:
        anomaly_pct = (
            self._total_anomalies / self._total_received * 100
            if self._total_received else 0
        )
        log.info(
            "📊 Recebidos: %d | Gravados: %d | Anomalias: %d (%.1f%%) | "
            "Inválidos: %d | Total: %.1f KB",
            self._total_received,
            self._total_written,
            self._total_anomalies,
            anomaly_pct,
            self._total_invalid,
            self._total_bytes / 1024,
        )

    def _print_final_stats(self) -> None:
        log.info("=" * 60)
        log.info("📈 Resumo da sessão")
        log.info("   Mensagens recebidas : %d", self._total_received)
        log.info("   Eventos gravados    : %d", self._total_written)
        log.info("   Anomalias detectadas: %d", self._total_anomalies)
        log.info("   Eventos inválidos   : %d", self._total_invalid)
        log.info("   Total gravado       : %.2f MB", self._total_bytes / 1_048_576)
        log.info("=" * 60)


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Consumer Kafka → Bronze (MinIO). "
                    "Compatível com sensor_simulator.py.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Kafka
    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
        help="Tópico a consumir (deve ser o mesmo do simulador)",
    )
    parser.add_argument(
        "--group-id",
        default=DEFAULT_GROUP_ID,
        help="Consumer group ID",
    )

    # MinIO / S3
    parser.add_argument(
        "--minio-endpoint",
        default=DEFAULT_MINIO_ENDPOINT,
        help="Endpoint HTTP do MinIO",
    )
    parser.add_argument(
        "--minio-access-key",
        default=DEFAULT_MINIO_ACCESS_KEY,
        help="Access key do MinIO",
    )
    parser.add_argument(
        "--minio-secret-key",
        default=DEFAULT_MINIO_SECRET_KEY,
        help="Secret key do MinIO",
    )
    parser.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET,
        help="Bucket Bronze no MinIO",
    )

    # Comportamento
    parser.add_argument(
        "--flush-size",
        type=int,
        default=DEFAULT_FLUSH_SIZE,
        help="Nº de eventos por partição antes de gravar no MinIO",
    )
    parser.add_argument(
        "--flush-interval",
        type=int,
        default=DEFAULT_FLUSH_INTERVAL,
        help="Intervalo máximo (s) entre gravações, mesmo com buffer incompleto",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Consome e valida, mas não grava no MinIO (útil para testes)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Nível de log",
    )

    args = parser.parse_args()

    # Ajustar log level
    logging.getLogger().setLevel(args.log_level)

    # Banner
    log.info("=" * 60)
    log.info("  Consumer Kafka → Bronze")
    log.info("  Tópico  : %s", args.topic)
    log.info("  Group   : %s", args.group_id)
    log.info("  Servers : %s", args.bootstrap_servers)
    log.info("  MinIO   : %s / bucket=%s", args.minio_endpoint, args.bucket)
    log.info("  Flush   : a cada %d eventos ou %ds", args.flush_size, args.flush_interval)
    log.info("  Dry-run : %s", args.dry_run)
    log.info("=" * 60)

    # Instanciar cliente MinIO
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
            log.error("Falha ao conectar ao MinIO: %s", exc)
            log.error("Dica: verifique se o MinIO está rodando em %s", args.minio_endpoint)
            sys.exit(1)

    # Instanciar e rodar consumer
    consumer = BronzeConsumer(
        bootstrap_servers = args.bootstrap_servers,
        topic             = args.topic,
        group_id          = args.group_id,
        minio_client      = minio_client,
        flush_size        = args.flush_size,
        flush_interval    = args.flush_interval,
        dry_run           = args.dry_run,
    )

    # Capturar SIGTERM (Docker stop)
    def _handle_sigterm(signum, frame):
        log.info("SIGTERM recebido — encerrando graciosamente...")
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _handle_sigterm)

    consumer.run()


if __name__ == "__main__":
    main()
