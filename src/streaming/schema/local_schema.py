"""Schema local — fallback usado quando o Schema Registry está offline."""

REQUIRED_FIELDS = frozenset({
    "event_id", "sensor_id", "equipment_id", "factory_id",
    "measurement_type", "value", "unit", "timestamp", "quality",
    "is_anomaly", "metadata",
})

VALID_QUALITIES    = frozenset({"good", "warning", "bad"})
VALID_MEASUREMENTS = frozenset({"temperature", "humidity", "pressure", "vibration", "current"})