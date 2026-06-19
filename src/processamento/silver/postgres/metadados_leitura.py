import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType

from processamento.silver.base_silver_etl import SilverETL
from utils.logger import logger


class MetadadosLeituraETL(SilverETL):
    """Bronze -> Silver para metadados de leitura (incremental).

    Tipagem de campos técnicos (bateria, sinal) e deduplicação por `leitura_id`.
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
            table_path="silver/postgres/metadados_leitura",
            execution_date=execution_date,
            owner=owner,
            environment=environment,
        )

    def extract(self) -> None:
        self.df = (
            self._read_bronze_parquet("postgres/metadados_leitura")
            .filter(col("_execution_date") == self.execution_date)
        )

    def transform(self) -> None:
        self.df = (
            self.df
            .withColumn("leitura_id", col("leitura_id").cast(StringType()))
            .withColumn("versao_firmware", col("versao_firmware").cast(StringType()))
            .withColumn("nivel_bateria", col("nivel_bateria").cast(IntegerType()))
            .withColumn("forca_sinal_dbm", col("forca_sinal_dbm").cast(IntegerType()))
            .filter(col("leitura_id").isNotNull())
        )
        self._deduplicate(["leitura_id"])

    def load(self) -> None:
        self.upsert_delta_table(
            unique_key_condition="target.leitura_id = source.leitura_id",
            partition_by=["_execution_date"],
            previous_delete=False,
        )

    def unit_tests(self) -> None:
        self.error_checks = (
            self.ErrorCheck
            .isComplete("leitura_id")
        )
        self.warning_checks = (
            self.WarningCheck
            .isComplete("versao_firmware")
            .isComplete("nivel_bateria")
            .isComplete("forca_sinal_dbm")
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Silver - PostgreSQL Metadados de Leitura")
    parser.add_argument("--execution_date", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--owner", required=True)
    params = vars(parser.parse_args())

    spark = (
        SparkSession.builder
        .appName("silver-postgres-metadados-leitura")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", ""))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", ""))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    logger.info("Iniciando pipeline Silver - PostgreSQL Metadados de Leitura")
    MetadadosLeituraETL(
        spark=spark,
        execution_date=params["execution_date"],
        owner=params["owner"],
        environment=params["environment"],
    ).run()
