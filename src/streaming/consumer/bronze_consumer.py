"""Consumer Bronze — entrypoint executável.

Uso:
    python -m streaming.consumer.bronze_consumer [opções]

Exemplos:
    python -m streaming.consumer.bronze_consumer
    python -m streaming.consumer.bronze_consumer --dry-run
    python -m streaming.consumer.bronze_consumer --schema-registry-url http://localhost:8081
"""
import argparse
import json
import logging
import signal
import sys
from datetime import datetime
from typing import Any
try:
    from dotenv import load_dotenv
    load_dotenv()  
except ImportError:
    pass 

try:
    from kafka import KafkaConsumer
except ImportError:
    print("Erro: kafka-python não instalado. Execute: pip install kafka-python")
    sys.exit(1)

try:
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:
    print("Erro: boto3 não instalado. Execute: pip install boto3")
    sys.exit(1)

from streaming.config import (
    DEFAULT_BOOTSTRAP_SERVERS, DEFAULT_TOPIC, DEFAULT_GROUP_ID,
    DEFAULT_MINIO_ENDPOINT, DEFAULT_MINIO_ROOT_USER, DEFAULT_MINIO_ROOT_PASSWORD,
    DEFAULT_BUCKET, DEFAULT_FLUSH_SIZE, DEFAULT_FLUSH_INTERVAL,
    DEFAULT_SCHEMA_REGISTRY_URL,
)
from streaming.buffer.partitioned_buffer import PartitionedBuffer
from streaming.consumer.base_consumer import BaseConsumer
from streaming.logging.json_formatter import setup_logging
from streaming.process.bronze_process import BronzeProcessor
from streaming.schema.schema_registry_client import SchemaRegistryClient
from streaming.storage.base_storage import StorageClient
from streaming.storage.dryrun_client import DryRunClient
from streaming.storage.minio_client import MinIOClient
from streaming.validation.schema_validator import SchemaValidator

log = setup_logging("bronze_consumer")


class BronzeConsumer(BaseConsumer):
    """
    Consome 'iot-sensors-raw' e persiste eventos brutos na camada Bronze
    com particionamento Hive-style. Commit manual de offset → at-least-once.
    """

    def _build_object_key(self, partition_path: str) -> str:
        ts_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        return f"{partition_path}/batch_{ts_str}.ndjson"

    def _flush(self) -> None:
        snapshot = self.buffer.drain()
        if not snapshot:
            return

        for partition_path, events in snapshot.items():
            if not events:
                continue
            key = self._build_object_key(partition_path)
            try:
                bytes_written = self.storage.put_json(key, events)
                self._total_written += len(events)
                self._total_bytes   += bytes_written
                log.info(
                    "Bronze gravado",
                    extra={
                        "key":    key,
                        "events": len(events),
                        "bytes":  bytes_written,
                        "kb":     round(bytes_written / 1024, 1),
                    }
                )
            except (BotoCoreError, ClientError) as exc:
                log.error("Falha ao gravar no storage", extra={"key": key, "error": str(exc)})
                raise

    def _flush_dlq(self) -> None:
        if not self._dlq:
            return
        ts_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        key    = f"_dead_letter/dt={datetime.utcnow():%Y-%m-%d}/invalid_{ts_str}.ndjson"
        try:
            self.storage.put_json(key, self._dlq)
            log.warning("DLQ gravada", extra={"events": len(self._dlq), "key": key})
            self._dlq = []
        except Exception as exc:
            log.error("Falha ao gravar DLQ", extra={"error": str(exc)})



def _build_kafka_consumer(bootstrap_servers: str, topic: str, group_id: str) -> Any:
    return KafkaConsumer(
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


def _build_storage(args: argparse.Namespace) -> StorageClient:
    if args.dry_run:
        log.info("Modo dry-run ativado — nada será gravado no MinIO")
        return DryRunClient()
    return MinIOClient(
        endpoint   = args.minio_endpoint,
        access_key = args.minio_root_user,
        secret_key = args.minio_root_password,
        bucket     = args.bucket,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consumer Kafka → Bronze (MinIO) com Schema Registry.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--bootstrap-servers",   default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic",               default=DEFAULT_TOPIC)
    parser.add_argument("--group-id",            default=DEFAULT_GROUP_ID)
    parser.add_argument("--minio-endpoint",      default=DEFAULT_MINIO_ENDPOINT)
    parser.add_argument(
        "--minio-root-user", "--minio-access-key",
        dest="minio_root_user",
        default=DEFAULT_MINIO_ROOT_USER,
        help="Root user MinIO / Access key S3",
    )
    parser.add_argument(
        "--minio-root-password", "--minio-secret-key",
        dest="minio_root_password",
        default=DEFAULT_MINIO_ROOT_PASSWORD,
        help="Root password MinIO / Secret key S3",
    )
    parser.add_argument("--bucket",              default=DEFAULT_BUCKET)
    parser.add_argument("--flush-size",          type=int, default=DEFAULT_FLUSH_SIZE)
    parser.add_argument("--flush-interval",      type=int, default=DEFAULT_FLUSH_INTERVAL)
    parser.add_argument("--schema-registry-url", default=DEFAULT_SCHEMA_REGISTRY_URL,
                        help="URL do Confluent Schema Registry")
    parser.add_argument("--dry-run",             action="store_true")
    parser.add_argument("--log-level",           default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    log.setLevel(getattr(logging, args.log_level.upper()))

    log.info("Iniciando Consumer Kafka → Bronze", extra={
        "topic":               args.topic,
        "group_id":            args.group_id,
        "bootstrap_servers":   args.bootstrap_servers,
        "minio_endpoint":      args.minio_endpoint,
        "bucket":              args.bucket,
        "flush_size":          args.flush_size,
        "flush_interval":      args.flush_interval,
        "schema_registry_url": args.schema_registry_url,
        "dry_run":             args.dry_run,
    })

    try:
        storage = _build_storage(args)
    except Exception as exc:
        log.error("Falha ao inicializar storage", extra={"error": str(exc)})
        sys.exit(1)

    registry  = SchemaRegistryClient(args.schema_registry_url, args.topic)
    validator = SchemaValidator(registry)
    processor = BronzeProcessor(validator)
    buffer    = PartitionedBuffer(args.flush_size, args.flush_interval)
    kafka_consumer = _build_kafka_consumer(args.bootstrap_servers, args.topic, args.group_id)

    consumer = BronzeConsumer(
        kafka_consumer = kafka_consumer,
        topic          = args.topic,
        buffer         = buffer,
        processor      = processor,
        storage        = storage,
    )

    # Graceful shutdown em SIGTERM (Docker stop, k8s rolling update, etc.)
    def _handle_sigterm(signum, frame):
        log.info("SIGTERM recebido — encerrando graciosamente")
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, _handle_sigterm)

    consumer.run()


if __name__ == "__main__":
    main()