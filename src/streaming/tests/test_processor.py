"""Testes de BronzeProcessor - usa fake message + validator real."""
import unittest
from types import SimpleNamespace

from streaming.process.bronze_process import BronzeProcessor, ProcessResult
from streaming.validation.schema_validator import SchemaValidator


def _msg(value, offset=10, partition=0, topic="iot-sensors-raw", key="k1"):
    return SimpleNamespace(
        value=value, offset=offset, partition=partition, topic=topic, key=key
    )


def _valid_payload(**overrides):
    base = {
        "event_id": "e1",
        "sensor_id": "s1",
        "equipment_id": "eq1",
        "factory_id": "F1",
        "measurement_type": "temperature",
        "value": 25.5,
        "unit": "celsius",
        "timestamp": "2025-01-01T10:00:00",
        "quality": "good",
        "is_anomaly": False,
        "metadata": {},
    }
    base.update(overrides)
    return base


class TestBronzeProcessor(unittest.TestCase):

    def setUp(self):
        self.processor = BronzeProcessor(SchemaValidator(registry_client=None))

    def test_valid_event_is_enriched(self):
        message = _msg(_valid_payload())
        result = self.processor.process(message)

        self.assertIsInstance(result, ProcessResult)
        self.assertTrue(result.is_valid)
        self.assertIsNone(result.reason)
        self.assertFalse(result.is_anomaly)

        evt = result.event
        self.assertEqual(evt["_kafka_offset"], 10)
        self.assertEqual(evt["_kafka_partition"], 0)
        self.assertEqual(evt["_kafka_topic"], "iot-sensors-raw")
        self.assertEqual(evt["_kafka_key"], "k1")
        self.assertIn("_ingested_at", evt)
        self.assertTrue(evt["_ingested_at"].endswith("Z"))

    def test_anomaly_flag_propagates(self):
        message = _msg(_valid_payload(is_anomaly=True))
        result = self.processor.process(message)
        self.assertTrue(result.is_valid)
        self.assertTrue(result.is_anomaly)

    def test_invalid_event_goes_to_dlq(self):
        bad_payload = _valid_payload()
        del bad_payload["sensor_id"]
        message = _msg(bad_payload)

        result = self.processor.process(message)
        self.assertFalse(result.is_valid)
        self.assertIsNotNone(result.reason)
        self.assertIn("sensor_id", result.reason)

        self.assertEqual(result.event["_dlq_reason"], result.reason)
        self.assertEqual(result.event["_kafka_offset"], 10)
        self.assertEqual(result.event["_kafka_partition"], 0)

    def test_invalid_event_does_not_have_ingested_at(self):
        """Eventos inválidos não devem ser enriquecidos com metadados além do mínimo."""
        bad = _valid_payload(value="bad")
        result = self.processor.process(_msg(bad))
        self.assertFalse(result.is_valid)
        self.assertNotIn("_ingested_at", result.event)


if __name__ == "__main__":
    unittest.main()