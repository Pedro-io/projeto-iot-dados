import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from processamento.abstract_etl import AbstractETL
from utils.logger import logger


class EquipamentosETL(AbstractETL):
    """ETL para extração de equipamentos do MongoDB para a camada Bronze.

    Lê a coleção `equipments` e achata os campos aninhados `factory` (struct)
    em colunas individuais. O array `sensors` é mantido como coluna estruturada
    para preservar a fidelidade da fonte na camada Bronze.
    """

    def __init__(
        self,
        spark: SparkSession,
        execution_date: str,
        owner: str,
        environment: str,
        mongo_uri: str,
        mongo_database: str,
        mongo_collection: str,
    ):
        super().__init__(
            spark=spark,
            table_path="bronze/mongo/equipamentos",
            execution_date=execution_date,
            owner=owner,
            environment=environment,
        )
        self.mongo_uri = mongo_uri
        self.mongo_database = mongo_database
        self.mongo_collection = mongo_collection

    def extract(self) -> None:
        """Extrai todos os documentos da coleção de equipamentos no MongoDB."""
        logger.info(f"Extraindo dados de {self.mongo_database}.{self.mongo_collection}")
        self.df = (
            self.spark.read
            .format("mongodb")
            .option("connection.uri", self.mongo_uri)
            .option("database", self.mongo_database)
            .option("collection", self.mongo_collection)
            .load()
        )

    def transform(self) -> None:
        """Achata o struct `factory` em colunas planas; mantém `sensors` como array."""
        self.df = (
            self.df
            .select(
                col("_id").alias("equipamento_id"),
                col("name").alias("nome"),
                col("type").alias("tipo"),
                col("factory.id").alias("fabrica_id"),
                col("factory.name").alias("fabrica_nome"),
                col("factory.location.lat").alias("latitude"),
                col("factory.location.lng").alias("longitude"),
                col("sensors"),
                col("maintenance_schedule").alias("periodicidade_manutencao"),
                col("installed_at").alias("instalado_em"),
                col("status"),
            )
        )

    def load(self) -> None:
        """Persiste os dados na camada Bronze em formato Parquet."""
        self.append_parquet()

    def unit_tests(self) -> None:
        """Verifica completude e consistência dos campos críticos."""
        self.error_checks = (
            self.ErrorCheck
            .isComplete("equipamento_id")
            .isComplete("nome")
            .isComplete("tipo")
            .isComplete("fabrica_id")
            .isComplete("status")
        )
        self.warning_checks = (
            self.WarningCheck
            .isComplete("instalado_em")
            .isComplete("latitude")
            .isComplete("longitude")
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Bronze - MongoDB Equipamentos")
    parser.add_argument("--execution_date", required=True, help="Data de execução (YYYY-MM-DD)")
    parser.add_argument("--environment", required=True, help="Ambiente de execução (hmg/prd)")
    parser.add_argument("--owner", required=True, help="Responsável pelo job")
    parser.add_argument("--mongo_uri", required=True, help="URI de conexão ao MongoDB (ex: mongodb://user:pass@host:27017)")
    parser.add_argument("--mongo_database", default="Database", help="Banco de dados MongoDB")
    parser.add_argument("--mongo_collection", default="equipments", help="Coleção MongoDB")
    params = vars(parser.parse_args())

    spark = (
        SparkSession.builder
        .appName("bronze-mongo-equipamentos")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", ""))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", ""))
        .getOrCreate()
    )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    logger.info("Iniciando pipeline Bronze - MongoDB Equipamentos")
    logger.info(f"Parâmetros de execução: {params}")

    EquipamentosETL(
        spark=spark,
        execution_date=params["execution_date"],
        owner=params["owner"],
        environment=params["environment"],
        mongo_uri=params["mongo_uri"],
        mongo_database=params["mongo_database"],
        mongo_collection=params["mongo_collection"],
    ).run()
