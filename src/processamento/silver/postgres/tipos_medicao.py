import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, IntegerType, StringType

from processamento.silver.base_silver_etl import SilverETL
from utils.logger import logger


class TiposMedicaoETL(SilverETL):
    """Bronze -> Silver para tipos de medição (tabela de referência, full load).

    Garante tipagem dos ranges de valor e deduplicação por `id`.
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
            table_path="silver/postgres/tipos_medicao",
            execution_date=execution_date,
            owner=owner,
            environment=environment,
        )

    def extract(self) -> None:
        self.df = self._read_bronze_parquet("postgres/tipos_medicao")

    def transform(self) -> None:
        self.df = (
            self.df
            .withColumn("id", col("id").cast(IntegerType()))
            .withColumn("nome", col("nome").cast(StringType()))
            .withColumn("unidade", col("unidade").cast(StringType()))
            .withColumn("valor_minimo", col("valor_minimo").cast(DoubleType()))
            .withColumn("valor_maximo", col("valor_maximo").cast(DoubleType()))
            .withColumn("faixa_normal_min", col("faixa_normal_min").cast(DoubleType()))
            .withColumn("faixa_normal_max", col("faixa_normal_max").cast(DoubleType()))
            .filter(col("id").isNotNull())
            .filter(col("valor_minimo") < col("valor_maximo"))
            .filter(col("faixa_normal_min") < col("faixa_normal_max"))
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
            .isComplete("unidade")
            .isComplete("valor_minimo")
            .isComplete("valor_maximo")
            .isComplete("faixa_normal_min")
            .isComplete("faixa_normal_max")
        )
        self.warning_checks = (
            self.WarningCheck
            .isUnique("nome")
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Silver - PostgreSQL Tipos de Medição")
    parser.add_argument("--execution_date", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--owner", required=True)
    params = vars(parser.parse_args())

    spark = (
        SparkSession.builder
        .appName("silver-postgres-tipos-medicao")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", ""))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", ""))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    logger.info("Iniciando pipeline Silver - PostgreSQL Tipos de Medição")
    TiposMedicaoETL(
        spark=spark,
        execution_date=params["execution_date"],
        owner=params["owner"],
        environment=params["environment"],
    ).run()
