"""Loop genérico de consumo Kafka - base para Bronze, Silver, Gold."""
import logging
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List

from streaming.buffer.partitioned_buffer import PartitionedBuffer
from streaming.process.bronze_process import BronzeProcessor
from streaming.storage.base_storage import StorageClient

log = logging.getLogger("bronze_consumer")


class BaseConsumer(ABC):
    """
    Loop e ciclo de vida genérico de um consumer Kafka.

    Subclasses implementam apenas:
      - _flush()      -> como gravar o buffer no destino
      - _flush_dlq()  -> como gravar a DLQ no destino

    Toda a coreografia de loop, commit manual de offset, shutdown gracioso
    e estatísticas vive aqui.
    """

    def __init__(
        self,
        kafka_consumer: Any,
        topic: str,
        buffer: PartitionedBuffer,
        processor: BronzeProcessor,
        storage: StorageClient,
    ):
        self.consumer  = kafka_consumer
        self.topic     = topic
        self.buffer    = buffer
        self.processor = processor
        self.storage   = storage

        # Contadores
        self._total_received  = 0
        self._total_written   = 0
        self._total_anomalies = 0
        self._total_invalid   = 0
        self._total_bytes     = 0
        self._session_start   = time.time()

        # Dead-letter buffer
        self._dlq: List[Dict[str, Any]] = []


    @abstractmethod
    def _flush(self) -> None:
        """Drena o buffer e persiste no storage."""

    @abstractmethod
    def _flush_dlq(self) -> None:
        """Persiste eventos da DLQ."""

    def run(self) -> None:
        # Import aqui para BaseConsumer não exigir kafka quando os testes
        # importam o módulo apenas para mockar.
        from kafka.errors import CommitFailedError

        log.info("Aguardando mensagens", extra={"topic": self.topic})

        try:
            for message in self.consumer:
                try:
                    self._handle_message(message)
                except Exception as exc:
                    log.error(
                        "Erro ao processar mensagem",
                        extra={"offset": getattr(message, "offset", None), "error": str(exc)}
                    )

                if self.buffer.should_flush():
                    try:
                        self._flush()
                        if self._dlq:
                            self._flush_dlq()
                        self.consumer.commit()
                        self._log_stats()
                    except Exception as exc:
                        log.error(
                            "Flush falhou - offset NÃO commitado",
                            extra={"error": str(exc)}
                        )

        except KeyboardInterrupt:
            log.info("Interrompido pelo usuário")
        except CommitFailedError as exc:
            log.error("Falha no commit de offset", extra={"error": str(exc)})
        finally:
            self._shutdown()

    def _handle_message(self, message: Any) -> None:
        self._total_received += 1
        result = self.processor.process(message)

        if not result.is_valid:
            log.warning(
                "Evento inválido - enviado para DLQ",
                extra={
                    "offset":    message.offset,
                    "partition": message.partition,
                    "kafka_key": message.key,
                    "reason":    result.reason,
                }
            )
            self._dlq.append(result.event)
            self._total_invalid += 1
            return

        if result.is_anomaly:
            self._total_anomalies += 1

        self.buffer.add(result.event)

    def _shutdown(self) -> None:
        log.info("Encerrando - gravando buffer restante")
        remaining = self.buffer.total()
        if remaining > 0 or self._dlq:
            try:
                self._flush()
                if self._dlq:
                    self._flush_dlq()
                self.consumer.commit()
                log.info("Buffer final commitado", extra={"events": remaining})
            except Exception as exc:
                log.error("Falha no flush final", extra={"error": str(exc)})

        try:
            self.consumer.close()
        except Exception:
            pass
        self._log_final_stats()


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