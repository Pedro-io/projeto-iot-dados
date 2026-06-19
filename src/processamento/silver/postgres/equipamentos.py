import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DateType, IntegerType, StringType

from processamento.silver.base_silver_etl import SilverETL
from utils.logger import logger


class EquipamentosETL(SilverETL):
    """Bronze -> Silver para equipamentos (tabela mestre, full load).

    Tipagem, deduplicação por `id`, validação de status e data de instalação.
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
            table_path="silver/postgres/equipamentos",
            execution_date=execution_date,
            owner=owner,
            environment=environment,
        )

    def extract(self) -> None:
        self.df = self._read_bronze_parquet("postgres/equipamentos")

    def transform(self) -> None:
        self.df = (
            self.df
            .withColumn("id", col("id").cast(StringType()))
            .withColumn("nome", col("nome").cast(StringType()))
            .withColumn("tipo_equipamento_id", col("tipo_equipamento_id").cast(IntegerType()))
            .withColumn("fabrica_id", col("fabrica_id").cast(StringType()))
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
            .isComplete("nome")
            .isComplete("tipo_equipamento_id")
            .isComplete("fabrica_id")
            .isComplete("status")
            .isContainedIn("status", ["ativo", "inativo", "manutencao"])
        )
        self.warning_checks = (
            self.WarningCheck
            .isComplete("data_instalacao")
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Silver - PostgreSQL Equipamentos")
    parser.add_argument("--execution_date", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--owner", required=True)
    params = vars(parser.parse_args())

    spark = (
        SparkSession.builder
        .appName("silver-postgres-equipamentos")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", ""))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", ""))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    logger.info("Iniciando pipeline Silver - PostgreSQL Equipamentos")
    EquipamentosETL(
        spark=spark,
        execution_date=params["execution_date"],
        owner=params["owner"],
        environment=params["environment"],
    ).run()
