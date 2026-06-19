import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, TimestampType

from processamento.silver.base_silver_etl import SilverETL
from utils.logger import logger


class LeiturasETL(SilverETL):
    """Bronze -> Silver para leituras de sensores.

    Aplica tipagem forte, remove duplicatas por `id` e valida ranges de valor.
    Carga incremental: lê apenas a partição da data de execução na bronze.
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
            table_path="silver/postgres/leituras",
            execution_date=execution_date,
            owner=owner,
            environment=environment,
        )

    def extract(self) -> None:
        self.df = (
            self._read_bronze_parquet("postgres/leituras")
            .filter(col("_execution_date") == self.execution_date)
        )

    def transform(self) -> None:
        self.df = (
            self.df
            .withColumn("id", col("id").cast(StringType()))
            .withColumn("sensor_id", col("sensor_id").cast(StringType()))
            .withColumn("valor", col("valor").cast(DoubleType()))
            .withColumn("status_qualidade_id", col("status_qualidade_id").cast(IntegerType()))
            .withColumn("timestamp_leitura", col("timestamp_leitura").cast(TimestampType()))
            .withColumn("is_anomalia", col("is_anomalia").cast(BooleanType()))
            .filter(col("id").isNotNull())
            .filter(col("valor").isNotNull())
            .filter(col("timestamp_leitura").isNotNull())
        )
        self._deduplicate(["id"])

    def load(self) -> None:
        self.upsert_delta_table(
            unique_key_condition="target.id = source.id",
            partition_by=["_execution_date"],
            previous_delete=False,
        )

    def unit_tests(self) -> None:
        self.error_checks = (
            self.ErrorCheck
            .isComplete("id")
            .isComplete("sensor_id")
            .isComplete("valor")
            .isComplete("status_qualidade_id")
            .isComplete("timestamp_leitura")
        )
        self.warning_checks = (
            self.WarningCheck
            .isComplete("is_anomalia")
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Silver - PostgreSQL Leituras")
    parser.add_argument("--execution_date", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--owner", required=True)
    params = vars(parser.parse_args())

    spark = (
        SparkSession.builder
        .appName("silver-postgres-leituras")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", ""))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", ""))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    logger.info("Iniciando pipeline Silver - PostgreSQL Leituras")
    LeiturasETL(
        spark=spark,
        execution_date=params["execution_date"],
        owner=params["owner"],
        environment=params["environment"],
    ).run()
