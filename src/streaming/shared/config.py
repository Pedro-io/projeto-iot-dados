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