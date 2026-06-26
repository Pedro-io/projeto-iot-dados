import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    lit,
    max,
    min,
    sum,
    to_date,
    when,
)

from processamento.gold.base_gold_etl import GoldETL
from utils.logger import logger


class FctAnomaliasDiaETL(GoldETL):
    """Silver (kafka/sensor_events) -> Gold fct_anomalias_dia.

    Agrega eventos de sensores por dia por sensor, calculando o total
    de leituras, total de anomalias e a taxa de anomalia do dia.
    Responde ao Req. 2 (Alertas de Anomalia) e Req. 3 (Relatórios Diários).
    Carga incremental pelo _execution_date da Silver.
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
            table_path="gold/fct/fct_anomalias_dia",
            execution_date=execution_date,
            owner=owner,
            environment=environment,
        )

    def extract(self) -> None:
        self.df = (
            self._read_silver_delta("kafka/sensor_events")
            .filter(col("_execution_date") == self.execution_date)
        )

    def transform(self) -> None:
        total_leituras = count(lit(1))
        total_anomalias = sum(when(col("is_anomaly") == True, 1).otherwise(0))

        self.df = (
            self.df
            .withColumn("data", to_date(col("timestamp")))
            .groupBy("data", "sensor_id", "equipment_id", "factory_id", "measurement_type")
            .agg(
                total_leituras.alias("total_leituras"),
                total_anomalias.alias("total_anomalias"),
                (
                    sum(when(col("is_anomaly") == True, 1.0).otherwise(0.0))
                    / count(lit(1))
                ).alias("taxa_anomalia"),
                max("value").alias("max_valor_dia"),
                min("value").alias("min_valor_dia"),
            )
            .filter(col("data").isNotNull())
        )

    def load(self) -> None:
        self.upsert_delta_table(
            unique_key_condition=(
                "target.data = source.data "
                "AND target.sensor_id = source.sensor_id"
            ),
            partition_by=["_execution_date"],
            previous_delete=True,
        )

    def unit_tests(self) -> None:
        self.error_checks = (
            self.ErrorCheck
            .isComplete("data")
            .isComplete("sensor_id")
            .isComplete("equipment_id")
            .isComplete("factory_id")
            .isComplete("measurement_type")
            .isComplete("total_leituras")
            .isComplete("total_anomalias")
            .isComplete("taxa_anomalia")
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Gold - Fato Anomalias por Dia")
    parser.add_argument("--execution_date", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--owner", required=True)
    params = vars(parser.parse_args())

    spark = (
        SparkSession.builder
        .appName("gold-fct-anomalias-dia")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", ""))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", ""))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    logger.info("Iniciando pipeline Gold - Fato Anomalias por Dia")
    FctAnomaliasDiaETL(
        spark=spark,
        execution_date=params["execution_date"],
        owner=params["owner"],
        environment=params["environment"],
    ).run()
