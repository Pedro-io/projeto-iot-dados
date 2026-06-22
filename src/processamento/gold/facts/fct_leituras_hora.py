import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    date_format,
    lit,
    max,
    min,
    stddev,
    sum,
    when,
)
from pyspark.sql.types import LongType

from processamento.gold.base_gold_etl import GoldETL
from utils.logger import logger


class FctLeiturasHoraETL(GoldETL):
    """Silver (kafka/sensor_events + postgres/tipos_medicao) -> Gold fct_leituras_hora.

    Agrega eventos de sensores por hora por sensor, calculando métricas
    estatísticas (avg, min, max, stddev), contagem de anomalias e percentual
    de qualidade boa. Join com tipos_medicao resolve measurement_type -> tipo_medicao_id.
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
            table_path="gold/fct/fct_leituras_hora",
            execution_date=execution_date,
            owner=owner,
            environment=environment,
        )

    def extract(self) -> None:
        self.df = (
            self._read_silver_delta("kafka/sensor_events")
            .filter(col("_execution_date") == self.execution_date)
        )
        self.df_tipos = self._read_silver_delta("postgres/tipos_medicao")

    def transform(self) -> None:
        tipos = self.df_tipos.select(
            col("id").alias("tipo_medicao_id"),
            col("nome").alias("tipo_nome"),
        )

        events = self.df.join(
            tipos,
            self.df["measurement_type"] == tipos["tipo_nome"],
            "left",
        ).drop("tipo_nome")

        self.df = (
            events
            .withColumn(
                "date_key",
                date_format(col("timestamp"), "yyyyMMddHH").cast(LongType()),
            )
            .groupBy(
                "date_key",
                "sensor_id",
                "equipment_id",
                "factory_id",
                "tipo_medicao_id",
                "measurement_type",
            )
            .agg(
                avg("value").alias("avg_valor"),
                min("value").alias("min_valor"),
                max("value").alias("max_valor"),
                stddev("value").alias("stddev_valor"),
                count(lit(1)).alias("count_leituras"),
                sum(
                    when(col("is_anomaly") == True, 1).otherwise(0)
                ).alias("count_anomalias"),
                (
                    sum(when(col("quality") == "good", 1.0).otherwise(0.0))
                    / count(lit(1))
                ).alias("pct_qualidade_boa"),
            )
            .filter(col("date_key").isNotNull())
        )

    def load(self) -> None:
        self.upsert_delta_table(
            unique_key_condition=(
                "target.date_key = source.date_key "
                "AND target.sensor_id = source.sensor_id"
            ),
            partition_by=["measurement_type", "_execution_date"],
            previous_delete=True,
        )

    def unit_tests(self) -> None:
        self.error_checks = (
            self.ErrorCheck
            .isComplete("date_key")
            .isComplete("sensor_id")
            .isComplete("equipment_id")
            .isComplete("factory_id")
            .isComplete("measurement_type")
            .isComplete("avg_valor")
            .isComplete("count_leituras")
        )
        self.warning_checks = (
            self.WarningCheck
            .isComplete("tipo_medicao_id")
            .isComplete("stddev_valor")
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Gold - Fato Leituras por Hora")
    parser.add_argument("--execution_date", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--owner", required=True)
    params = vars(parser.parse_args())

    spark = (
        SparkSession.builder
        .appName("gold-fct-leituras-hora")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", ""))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", ""))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    logger.info("Iniciando pipeline Gold - Fato Leituras por Hora")
    FctLeiturasHoraETL(
        spark=spark,
        execution_date=params["execution_date"],
        owner=params["owner"],
        environment=params["environment"],
    ).run()
