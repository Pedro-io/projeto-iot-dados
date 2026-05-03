"""Testes de BaseConsumer / BronzeConsumer com Kafka e Storage mockados."""
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from streaming.buffer.partitioned_buffer import PartitionedBuffer
from streaming.consumer.bronze_consumer import BronzeConsumer
from streaming.process.bronze_process import BronzeProcessor
from streaming.storage.dryrun_client import DryRunClient
from streaming.validation.schema_validator import SchemaValidator


def _msg(payload, offset=0, partition=0):
    return SimpleNamespace(
        value=payload, offset=offset, partition=partition,
        topic="iot-sensors-raw", key=f"k{offset}",
    )


def _valid_payload(**ov):
    base = {
        "event_id": "e1", "sensor_id": "s1", "equipment_id": "eq1",
        "factory_id": "F1", "measurement_type": "temperature",
        "value": 25.5, "unit": "celsius", "timestamp": "2025-01-01T10:00:00",
        "quality": "good", "is_anomaly": False, "metadata": {},
    }
    base.update(ov)
    return base


class FakeKafkaConsumer:
    """Iterador finito de mensagens, com .commit() e .close() mockados."""

    def __init__(self, messages):
        self._messages = messages
        self.commit = MagicMock()
        self.close  = MagicMock()

    def __iter__(self):
        return iter(self._messages)


class TestBronzeConsumer(unittest.TestCase):

    def _build(self, messages, flush_size=2, flush_interval=999):
        kafka  = FakeKafkaConsumer(messages)
        buffer = PartitionedBuffer(flush_size=flush_size, flush_interval=flush_interval)
        proc   = BronzeProcessor(SchemaValidator(registry_client=None))
        store  = DryRunClient()

        consumer = BronzeConsumer(
            kafka_consumer=kafka, topic="iot-sensors-raw",
            buffer=buffer, processor=proc, storage=store,
        )
        return consumer, kafka

    def test_processes_valid_messages(self):
        msgs = [_msg(_valid_payload(), offset=i) for i in range(3)]
        consumer, kafka = self._build(msgs, flush_size=2)
        consumer.run()
        self.assertEqual(consumer._total_received, 3)
        self.assertEqual(consumer._total_invalid, 0)
        self.assertEqual(consumer._total_written, 3)
        self.assertGreaterEqual(kafka.commit.call_count, 1)
        kafka.close.assert_called_once()

    def test_invalid_messages_go_to_dlq(self):
        bad = _valid_payload()
        del bad["sensor_id"]
        msgs = [_msg(bad, offset=0), _msg(_valid_payload(), offset=1)]
        consumer, _ = self._build(msgs, flush_size=10)
        consumer.run()
        self.assertEqual(consumer._total_received, 2)
        self.assertEqual(consumer._total_invalid, 1)

    def test_anomaly_count(self):
        msgs = [
            _msg(_valid_payload(is_anomaly=True), offset=0),
            _msg(_valid_payload(is_anomaly=False), offset=1),
            _msg(_valid_payload(is_anomaly=True), offset=2),
        ]
        consumer, _ = self._build(msgs, flush_size=10)
        consumer.run()
        self.assertEqual(consumer._total_anomalies, 2)

    def test_final_flush_on_shutdown(self):
        """Mesmo sem atingir flush_size, mensagens pendentes devem ser gravadas."""
        msgs = [_msg(_valid_payload(), offset=i) for i in range(3)]
        consumer, kafka = self._build(msgs, flush_size=999)
        consumer.run()
        self.assertEqual(consumer._total_written, 3)
        self.assertGreaterEqual(kafka.commit.call_count, 1)


if __name__ == "__main__":
    unittest.main()