from streaming.buffer.partitioned_buffer import PartitionedBuffer
from streaming.consumers.bronze_consumer import BronzeConsumer
from streaming.consumers.kafka_consumer import KafkaMessageConsumer
from streaming.storage.minio import MinIOStorage
from streaming.validation.validator import SchemaValidator


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
 
 
consumer = KafkaMessageConsumer(
    bootstrap_servers=DEFAULT_BOOTSTRAP_SERVERS,
    topic=DEFAULT_TOPIC,
    group_id=DEFAULT_GROUP_ID
)
storage = MinIOStorage(
    endpoint=DEFAULT_MINIO_ENDPOINT,
    access_key=DEFAULT_MINIO_ACCESS_KEY,
    secret_key=DEFAULT_MINIO_SECRET_KEY,
    bucket=DEFAULT_BUCKET
)
validator = SchemaValidator(
    schema_registry_url=DEFAULT_SCHEMA_REGISTRY_URL,
    required_fields=REQUIRED_FIELDS,
    valid_qualities=VALID_QUALITIES,
    valid_measurements=VALID_MEASUREMENTS
)
buffer = PartitionedBuffer(
    flush_size=DEFAULT_FLUSH_SIZE,
    flush_interval=DEFAULT_FLUSH_INTERVAL
)

app = BronzeConsumer(consumer, storage, validator, buffer)

app.run()