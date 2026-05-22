"""Logging estruturado em JSON Lines.

Compatível com Elasticsearch, Loki, CloudWatch e qualquer stack de
observabilidade que ingere JSON.
"""
import json
import logging
import sys
from datetime import datetime


class JsonFormatter(logging.Formatter):
    """
    Formata logs como JSON Lines — cada linha é um objeto JSON independente.

    Exemplo de saída:
        {"timestamp": "2025-03-29T14:30:00Z", "level": "INFO",
         "logger": "bronze_consumer", "message": "Consumer iniciado",
         "topic": "iot-sensors-raw"}
    """

    _RESERVED_ATTRS = frozenset({
        "name", "msg", "args", "levelname", "levelno", "pathname",
        "filename", "module", "exc_info", "exc_text", "stack_info",
        "lineno", "funcName", "created", "msecs", "relativeCreated",
        "thread", "threadName", "processName", "process", "message",
        "taskName",
    })

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "level":     record.levelname,
            "logger":    record.name,
            "message":   record.getMessage(),
        }

        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        for key, value in record.__dict__.items():
            if key not in self._RESERVED_ATTRS and not key.startswith("_"):
                log_entry[key] = value

        return json.dumps(log_entry, ensure_ascii=False, default=str)


def setup_logging(name: str = "bronze_consumer", level: str = "INFO") -> logging.Logger:
    """Configura logger com saída JSON estruturada em stdout."""
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)

    for noisy in ("kafka", "boto3", "botocore", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    return logger