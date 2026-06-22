import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from processamento.gold.base_gold_etl import GoldETL
from utils.logger import logger


class DimFabricaETL(GoldETL):
    """Silver (postgres/fabricas) -> Gold dim_fabrica.

    SCD Tipo 1: upsert por fabrica_id sobrescreve nome e coordenadas se alterados.
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
            table_path="gold/dim/dim_fabrica",
            execution_date=execution_date,
            owner=owner,
            environment=environment,
        )

    def extract(self) -> None:
        self.df = self._read_silver_delta("postgres/fabricas")

    def transform(self) -> None:
        self.df = (
            self.df
            .select(
                col("id").alias("fabrica_id"),
                col("nome"),
                col("latitude"),
                col("longitude"),
            )
            .filter(col("fabrica_id").isNotNull())
            .dropDuplicates(["fabrica_id"])
        )

    def load(self) -> None:
        self.upsert_delta_table(
            unique_key_condition="target.fabrica_id = source.fabrica_id",
            previous_delete=False,
        )

    def unit_tests(self) -> None:
        self.error_checks = (
            self.ErrorCheck
            .isComplete("fabrica_id")
            .isComplete("nome")
            .isComplete("latitude")
            .isComplete("longitude")
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Gold - Dimensão Fábrica")
    parser.add_argument("--execution_date", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--owner", required=True)
    params = vars(parser.parse_args())

    spark = (
        SparkSession.builder
        .appName("gold-dim-fabrica")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", ""))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", ""))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    logger.info("Iniciando pipeline Gold - Dimensão Fábrica")
    DimFabricaETL(
        spark=spark,
        execution_date=params["execution_date"],
        owner=params["owner"],
        environment=params["environment"],
    ).run()
