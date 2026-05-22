"""Testes de SchemaValidator usando schema local (registry=None)."""
import unittest

from streaming.validation.schema_validator import SchemaValidator


def _valid_event(**overrides):
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
        "metadata": {"foo": "bar"},
    }
    base.update(overrides)
    return base


class TestSchemaValidator(unittest.TestCase):

    def setUp(self):
        # registry=None → fallback total para schema local
        self.validator = SchemaValidator(registry_client=None)

    def test_valid_event(self):
        valid, reason = self.validator.validate(_valid_event())
        self.assertTrue(valid, reason)
        self.assertIsNone(reason)

    def test_missing_required_field(self):
        evt = _valid_event()
        del evt["sensor_id"]
        valid, reason = self.validator.validate(evt)
        self.assertFalse(valid)
        self.assertIn("sensor_id", reason)

    def test_value_must_be_numeric(self):
        valid, reason = self.validator.validate(_valid_event(value="not-a-number"))
        self.assertFalse(valid)
        self.assertIn("value", reason)

    def test_value_accepts_int_and_float(self):
        for v in (1, 1.5, -5, 0):
            valid, _ = self.validator.validate(_valid_event(value=v))
            self.assertTrue(valid, f"failed for value={v}")

    def test_is_anomaly_must_be_bool(self):
        valid, reason = self.validator.validate(_valid_event(is_anomaly="yes"))
        self.assertFalse(valid)
        self.assertIn("is_anomaly", reason)

    def test_invalid_quality(self):
        valid, reason = self.validator.validate(_valid_event(quality="excellent"))
        self.assertFalse(valid)
        self.assertIn("quality", reason)

    def test_valid_qualities(self):
        for q in ("good", "warning", "bad"):
            valid, _ = self.validator.validate(_valid_event(quality=q))
            self.assertTrue(valid, f"failed for quality={q}")

    def test_invalid_measurement_type(self):
        valid, reason = self.validator.validate(_valid_event(measurement_type="xyz"))
        self.assertFalse(valid)
        self.assertIn("measurement_type", reason)

    def test_metadata_must_be_dict(self):
        valid, reason = self.validator.validate(_valid_event(metadata="string"))
        self.assertFalse(valid)
        self.assertIn("metadata", reason)

    def test_non_dict_event(self):
        valid, reason = self.validator.validate("not-a-dict")
        self.assertFalse(valid)


if __name__ == "__main__":
    unittest.main()