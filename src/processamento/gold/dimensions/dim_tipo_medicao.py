import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from processamento.gold.base_gold_etl import GoldETL
from utils.logger import logger


class DimTipoMedicaoETL(GoldETL):
    """Silver (postgres/tipos_medicao) -> Gold dim_tipo_medicao.

    SCD Tipo 1: upsert por tipo_medicao_id. Preserva os limites físicos
    e faixas operacionais para uso em detecção de anomalias na Gold.
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
            table_path="gold/dim/dim_tipo_medicao",
            execution_date=execution_date,
            owner=owner,
            environment=environment,
        )

    def extract(self) -> None:
        self.df = self._read_silver_delta("postgres/tipos_medicao")

    def transform(self) -> None:
        self.df = (
            self.df
            .select(
                col("id").alias("tipo_medicao_id"),
                col("nome"),
                col("unidade"),
                col("valor_minimo"),
                col("valor_maximo"),
                col("faixa_normal_min"),
                col("faixa_normal_max"),
            )
            .filter(col("tipo_medicao_id").isNotNull())
            .dropDuplicates(["tipo_medicao_id"])
        )

    def load(self) -> None:
        self.upsert_delta_table(
            unique_key_condition="target.tipo_medicao_id = source.tipo_medicao_id",
            previous_delete=False,
        )

    def unit_tests(self) -> None:
        self.error_checks = (
            self.ErrorCheck
            .isComplete("tipo_medicao_id")
            .isComplete("nome")
            .isComplete("unidade")
            .isComplete("valor_minimo")
            .isComplete("valor_maximo")
            .isComplete("faixa_normal_min")
            .isComplete("faixa_normal_max")
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Gold - Dimensão Tipo de Medição")
    parser.add_argument("--execution_date", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--owner", required=True)
    params = vars(parser.parse_args())

    spark = (
        SparkSession.builder
        .appName("gold-dim-tipo-medicao")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", ""))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", ""))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    logger.info("Iniciando pipeline Gold - Dimensão Tipo de Medição")
    DimTipoMedicaoETL(
        spark=spark,
        execution_date=params["execution_date"],
        owner=params["owner"],
        environment=params["environment"],
    ).run()
