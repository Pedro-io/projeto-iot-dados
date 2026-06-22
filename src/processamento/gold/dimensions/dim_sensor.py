import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from processamento.gold.base_gold_etl import GoldETL
from utils.logger import logger


class DimSensorETL(GoldETL):
    """Silver (postgres/sensores) -> Gold dim_sensor.

    SCD Tipo 1: upsert por sensor_id.
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
            table_path="gold/dim/dim_sensor",
            execution_date=execution_date,
            owner=owner,
            environment=environment,
        )

    def extract(self) -> None:
        self.df = self._read_silver_delta("postgres/sensores")

    def transform(self) -> None:
        self.df = (
            self.df
            .select(
                col("id").alias("sensor_id"),
                col("equipamento_id").alias("equipment_id"),
                col("tipo_medicao_id"),
                col("status"),
                col("data_instalacao"),
            )
            .filter(col("sensor_id").isNotNull())
            .dropDuplicates(["sensor_id"])
        )

    def load(self) -> None:
        self.upsert_delta_table(
            unique_key_condition="target.sensor_id = source.sensor_id",
            previous_delete=False,
        )

    def unit_tests(self) -> None:
        self.error_checks = (
            self.ErrorCheck
            .isComplete("sensor_id")
            .isComplete("equipment_id")
            .isComplete("tipo_medicao_id")
            .isComplete("status")
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Gold - Dimensão Sensor")
    parser.add_argument("--execution_date", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--owner", required=True)
    params = vars(parser.parse_args())

    spark = (
        SparkSession.builder
        .appName("gold-dim-sensor")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", ""))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", ""))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    logger.info("Iniciando pipeline Gold - Dimensão Sensor")
    DimSensorETL(
        spark=spark,
        execution_date=params["execution_date"],
        owner=params["owner"],
        environment=params["environment"],
    ).run()
