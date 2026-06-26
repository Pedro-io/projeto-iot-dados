import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, StringType

from processamento.silver.base_silver_etl import SilverETL
from utils.logger import logger


class EquipamentosETL(SilverETL):
    """Bronze -> Silver para equipamentos do MongoDB (full load).

    Aplica tipagem nos campos planos e deduplicação por `equipamento_id`.
    O array `sensors` é mantido estruturado para uso na Gold/análises.
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
            table_path="silver/mongo/equipamentos",
            execution_date=execution_date,
            owner=owner,
            environment=environment,
        )

    def extract(self) -> None:
        self.df = self._read_bronze_parquet("mongo/equipamentos")

    def transform(self) -> None:
        self.df = (
            self.df
            .withColumn("equipamento_id", col("equipamento_id").cast(StringType()))
            .withColumn("nome", col("nome").cast(StringType()))
            .withColumn("tipo", col("tipo").cast(StringType()))
            .withColumn("fabrica_id", col("fabrica_id").cast(StringType()))
            .withColumn("fabrica_nome", col("fabrica_nome").cast(StringType()))
            .withColumn("latitude", col("latitude").cast(DoubleType()))
            .withColumn("longitude", col("longitude").cast(DoubleType()))
            .withColumn("status", col("status").cast(StringType()))
            .withColumn("periodicidade_manutencao", col("periodicidade_manutencao").cast(StringType()))
            .filter(col("equipamento_id").isNotNull())
            .filter(col("status").isin(["active", "inactive", "maintenance"]))
        )
        self._deduplicate(["equipamento_id"])

    def load(self) -> None:
        self.upsert_delta_table(
            unique_key_condition="target.equipamento_id = source.equipamento_id",
            previous_delete=False,
        )

    def unit_tests(self) -> None:
        self.error_checks = (
            self.ErrorCheck
            .isComplete("equipamento_id")
            .isComplete("nome")
            .isComplete("tipo")
            .isComplete("fabrica_id")
            .isComplete("status")
            .isContainedIn("status", ["active", "inactive", "maintenance"])
        )
        self.warning_checks = (
            self.WarningCheck
            .isComplete("latitude")
            .isComplete("longitude")
            .isComplete("periodicidade_manutencao")
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Silver - MongoDB Equipamentos")
    parser.add_argument("--execution_date", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--owner", required=True)
    params = vars(parser.parse_args())

    spark = (
        SparkSession.builder
        .appName("silver-mongo-equipamentos")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", ""))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", ""))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    logger.info("Iniciando pipeline Silver - MongoDB Equipamentos")
    EquipamentosETL(
        spark=spark,
        execution_date=params["execution_date"],
        owner=params["owner"],
        environment=params["environment"],
    ).run()
