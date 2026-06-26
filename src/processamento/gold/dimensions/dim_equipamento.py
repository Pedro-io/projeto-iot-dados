import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from processamento.gold.base_gold_etl import GoldETL
from utils.logger import logger


class DimEquipamentoETL(GoldETL):
    """Silver (postgres/equipamentos + tipos_equipamento + mongo/equipamentos) -> Gold dim_equipamento.

    SCD Tipo 1: join triplo para enriquecer equipamentos com o nome do tipo
    (postgres) e com a periodicidade de manutenção (mongo).
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
            table_path="gold/dim/dim_equipamento",
            execution_date=execution_date,
            owner=owner,
            environment=environment,
        )

    def extract(self) -> None:
        self.df = self._read_silver_delta("postgres/equipamentos")
        self.df_tipos = self._read_silver_delta("postgres/tipos_equipamento")
        self.df_mongo = self._read_silver_delta("mongo/equipamentos")

    def transform(self) -> None:
        equip = self.df.select(
            "id", "nome", "tipo_equipamento_id", "fabrica_id", "status", "data_instalacao"
        )
        tipos = self.df_tipos.select(
            col("id").alias("tipo_id"),
            col("nome").alias("tipo_equipamento"),
        )
        mongo = self.df_mongo.select(
            col("equipamento_id").alias("mongo_id"),
            "periodicidade_manutencao",
        )

        self.df = (
            equip
            .join(tipos, equip["tipo_equipamento_id"] == tipos["tipo_id"], "left")
            .join(mongo, equip["id"] == mongo["mongo_id"], "left")
            .drop("tipo_id", "mongo_id", "tipo_equipamento_id")
            .withColumnRenamed("id", "equipment_id")
            .filter(col("equipment_id").isNotNull())
            .dropDuplicates(["equipment_id"])
        )

    def load(self) -> None:
        self.upsert_delta_table(
            unique_key_condition="target.equipment_id = source.equipment_id",
            previous_delete=False,
        )

    def unit_tests(self) -> None:
        self.error_checks = (
            self.ErrorCheck
            .isComplete("equipment_id")
            .isComplete("nome")
            .isComplete("fabrica_id")
            .isComplete("status")
        )
        self.warning_checks = (
            self.WarningCheck
            .isComplete("tipo_equipamento")
            .isComplete("periodicidade_manutencao")
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Gold - Dimensão Equipamento")
    parser.add_argument("--execution_date", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--owner", required=True)
    params = vars(parser.parse_args())

    spark = (
        SparkSession.builder
        .appName("gold-dim-equipamento")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", ""))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", ""))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    logger.info("Iniciando pipeline Gold - Dimensão Equipamento")
    DimEquipamentoETL(
        spark=spark,
        execution_date=params["execution_date"],
        owner=params["owner"],
        environment=params["environment"],
    ).run()
