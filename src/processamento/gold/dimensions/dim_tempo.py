import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    dayofmonth,
    dayofweek,
    lit,
    month,
    weekofyear,
    when,
    year,
)
from pyspark.sql.types import DateType, IntegerType, LongType

from processamento.gold.base_gold_etl import GoldETL
from utils.logger import logger


class DimTempoETL(GoldETL):
    """Gera a dimensão de tempo para a execução do dia.

    Produz 24 linhas (uma por hora) com todos os atributos calendáriais
    derivados da execution_date. Upsert por date_key garante idempotência.
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
            table_path="gold/dim/dim_tempo",
            execution_date=execution_date,
            owner=owner,
            environment=environment,
        )

    def extract(self) -> None:
        exec_date = self._parse_execution_date()
        self.df = (
            self.spark.range(24)
            .withColumnRenamed("id", "hora")
            .withColumn("hora", col("hora").cast(IntegerType()))
            .withColumn("data", lit(exec_date).cast(DateType()))
        )

    def transform(self) -> None:
        nome_mes_col = (
            when(month(col("data")) == 1, "Janeiro")
            .when(month(col("data")) == 2, "Fevereiro")
            .when(month(col("data")) == 3, "Março")
            .when(month(col("data")) == 4, "Abril")
            .when(month(col("data")) == 5, "Maio")
            .when(month(col("data")) == 6, "Junho")
            .when(month(col("data")) == 7, "Julho")
            .when(month(col("data")) == 8, "Agosto")
            .when(month(col("data")) == 9, "Setembro")
            .when(month(col("data")) == 10, "Outubro")
            .when(month(col("data")) == 11, "Novembro")
            .otherwise("Dezembro")
        )

        self.df = (
            self.df
            .withColumn(
                "date_key",
                (
                    year(col("data")) * 1000000
                    + month(col("data")) * 10000
                    + dayofmonth(col("data")) * 100
                    + col("hora")
                ).cast(LongType()),
            )
            .withColumn("dia_semana", dayofweek(col("data")).cast(IntegerType()))
            .withColumn("semana_ano", weekofyear(col("data")).cast(IntegerType()))
            .withColumn("mes", month(col("data")).cast(IntegerType()))
            .withColumn(
                "trimestre",
                when(month(col("data")) <= 3, 1)
                .when(month(col("data")) <= 6, 2)
                .when(month(col("data")) <= 9, 3)
                .otherwise(4),
            )
            .withColumn("ano", year(col("data")).cast(IntegerType()))
            .withColumn("nome_mes", nome_mes_col)
            .select(
                "date_key", "data", "hora", "dia_semana",
                "semana_ano", "mes", "trimestre", "ano", "nome_mes",
            )
        )

    def load(self) -> None:
        self.upsert_delta_table(
            unique_key_condition="target.date_key = source.date_key",
            previous_delete=False,
        )

    def unit_tests(self) -> None:
        self.error_checks = (
            self.ErrorCheck
            .isComplete("date_key")
            .isComplete("data")
            .isComplete("hora")
            .isComplete("mes")
            .isComplete("ano")
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Gold - Dimensão Tempo")
    parser.add_argument("--execution_date", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--owner", required=True)
    params = vars(parser.parse_args())

    spark = (
        SparkSession.builder
        .appName("gold-dim-tempo")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", ""))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", ""))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    logger.info("Iniciando pipeline Gold - Dimensão Tempo")
    DimTempoETL(
        spark=spark,
        execution_date=params["execution_date"],
        owner=params["owner"],
        environment=params["environment"],
    ).run()
