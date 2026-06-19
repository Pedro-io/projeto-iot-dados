import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    TimestampType,
)

from processamento.silver.base_silver_etl import SilverETL
from utils.logger import logger

# Valores válidos conforme definidos no schema local da camada Bronze
VALID_QUALITIES = ["good", "warning", "bad"]
VALID_MEASUREMENT_TYPES = ["temperature", "humidity", "pressure", "vibration", "current"]


class SensorEventsETL(SilverETL):
    """Bronze (Kafka NDJSON) -> Silver para eventos de sensores IoT.

    Lê os NDJSON particionados por factory_id/measurement_type/dt na bronze,
    aplica tipagem, achata o objeto `metadata`, deduplica por `event_id`
    e remove campos exclusivos do transporte Kafka.
    Carga incremental pela partição dt=execution_date.
    """

    def __init__(
        self,
        spark: SparkSession,
        execution_date: str,
        owner: str,
        environment: str,
    ):
        super().__init__(
            spark=spark,
            table_path="silver/kafka/sensor_events",
            execution_date=execution_date,
            owner=owner,
            environment=environment,
        )

    def extract(self) -> None:
        """Lê NDJSON do dia de execução via glob nas partições Hive da bronze."""
        path = (
            f"s3a://{self.bronze_bucket}/"
            f"factory_id=*/measurement_type=*/dt={self.execution_date}/*"
        )
        logger.info(f"Lendo bronze NDJSON: {path}")
        self.df = (
            self.spark.read
            .option("basePath", f"s3a://{self.bronze_bucket}/")
            .json(path)
        )

    def transform(self) -> None:
        self.df = (
            self.df
            .withColumn("event_id", col("event_id").cast(StringType()))
            .withColumn("sensor_id", col("sensor_id").cast(StringType()))
            .withColumn("equipment_id", col("equipment_id").cast(StringType()))
            .withColumn("factory_id", col("factory_id").cast(StringType()))
            .withColumn("measurement_type", col("measurement_type").cast(StringType()))
            .withColumn("value", col("value").cast(DoubleType()))
            .withColumn("unit", col("unit").cast(StringType()))
            .withColumn("timestamp", col("timestamp").cast(TimestampType()))
            .withColumn("quality", col("quality").cast(StringType()))
            .withColumn("is_anomaly", col("is_anomaly").cast(BooleanType()))
            # Achata metadata: firmware e bateria vêm do simulador de sensores
            .withColumn("versao_firmware", col("metadata.firmware_version").cast(StringType()))
            .withColumn("nivel_bateria", col("metadata.battery_level").cast(IntegerType()))
            # Mantém _ingested_at para rastreabilidade; descarta metadados de transporte
            .withColumn("_ingested_at", col("_ingested_at").cast(TimestampType()))
            .drop("metadata", "_kafka_offset", "_kafka_partition", "_kafka_topic", "_kafka_key")
            .filter(col("event_id").isNotNull())
            .filter(col("value").isNotNull())
            .filter(col("timestamp").isNotNull())
            .filter(col("quality").isin(VALID_QUALITIES))
            .filter(col("measurement_type").isin(VALID_MEASUREMENT_TYPES))
        )
        self._deduplicate(["event_id"])

    def load(self) -> None:
        self.upsert_delta_table(
            unique_key_condition="target.event_id = source.event_id",
            partition_by=["measurement_type", "_execution_date"],
            previous_delete=False,
        )

    def unit_tests(self) -> None:
        self.error_checks = (
            self.ErrorCheck
            .isComplete("event_id")
            .isComplete("sensor_id")
            .isComplete("equipment_id")
            .isComplete("factory_id")
            .isComplete("measurement_type")
            .isComplete("value")
            .isComplete("timestamp")
            .isComplete("quality")
            .isContainedIn("quality", VALID_QUALITIES)
            .isContainedIn("measurement_type", VALID_MEASUREMENT_TYPES)
        )
        self.warning_checks = (
            self.WarningCheck
            .isComplete("is_anomaly")
            .isComplete("versao_firmware")
            .isComplete("nivel_bateria")
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Silver - Kafka Sensor Events")
    parser.add_argument("--execution_date", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--owner", required=True)
    params = vars(parser.parse_args())

    spark = (
        SparkSession.builder
        .appName("silver-kafka-sensor-events")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", ""))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", ""))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    logger.info("Iniciando pipeline Silver - Kafka Sensor Events")
    SensorEventsETL(
        spark=spark,
        execution_date=params["execution_date"],
        owner=params["owner"],
        environment=params["environment"],
    ).run()
