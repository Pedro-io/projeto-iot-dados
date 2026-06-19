import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DateType, IntegerType, StringType

from processamento.silver.base_silver_etl import SilverETL
from utils.logger import logger


class SensoresETL(SilverETL):
    """Bronze -> Silver para sensores (tabela mestre, full load).

    Tipagem, deduplicação por `id` e validação de status operacional.
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
            table_path="silver/postgres/sensores",
            execution_date=execution_date,
            owner=owner,
            environment=environment,
        )

    def extract(self) -> None:
        self.df = self._read_bronze_parquet("postgres/sensores")

    def transform(self) -> None:
        self.df = (
            self.df
            .withColumn("id", col("id").cast(StringType()))
            .withColumn("equipamento_id", col("equipamento_id").cast(StringType()))
            .withColumn("tipo_medicao_id", col("tipo_medicao_id").cast(IntegerType()))
            .withColumn("status", col("status").cast(StringType()))
            .withColumn("data_instalacao", col("data_instalacao").cast(DateType()))
            .filter(col("id").isNotNull())
            .filter(col("status").isin(["ativo", "inativo", "manutencao"]))
        )
        self._deduplicate(["id"])

    def load(self) -> None:
        self.upsert_delta_table(
            unique_key_condition="target.id = source.id",
            previous_delete=False,
        )

    def unit_tests(self) -> None:
        self.error_checks = (
            self.ErrorCheck
            .isComplete("id")
            .isComplete("equipamento_id")
            .isComplete("tipo_medicao_id")
            .isComplete("status")
            .isContainedIn("status", ["ativo", "inativo", "manutencao"])
        )
        self.warning_checks = (
            self.WarningCheck
            .isComplete("data_instalacao")
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Silver - PostgreSQL Sensores")
    parser.add_argument("--execution_date", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--owner", required=True)
    params = vars(parser.parse_args())

    spark = (
        SparkSession.builder
        .appName("silver-postgres-sensores")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", ""))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", ""))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    logger.info("Iniciando pipeline Silver - PostgreSQL Sensores")
    SensoresETL(
        spark=spark,
        execution_date=params["execution_date"],
        owner=params["owner"],
        environment=params["environment"],
    ).run()
