import argparse
import os

from pyspark.sql import SparkSession

from processamento.abstract_etl import AbstractETL
from utils.logger import logger


class StatusQualidadeETL(AbstractETL):
    """ETL para extração da tabela de domínio `status_qualidade` do PostgreSQL.

    Carga completa (full load) - tabela de referência com os possíveis status
    de qualidade de uma leitura de sensor (good, warning, bad).
    """

    def __init__(
        self,
        spark: SparkSession,
        execution_date: str,
        owner: str,
        environment: str,
        jdbc_url: str,
        jdbc_user: str,
        jdbc_password: str,
    ):
        super().__init__(
            spark=spark,
            table_path="bronze/postgres/status_qualidade",
            execution_date=execution_date,
            owner=owner,
            environment=environment,
        )
        self.jdbc_url = jdbc_url
        self.jdbc_user = jdbc_user
        self.jdbc_password = jdbc_password

    def _jdbc_read(self, dbtable: str):
        """Lê uma tabela ou subquery do PostgreSQL via JDBC."""
        return (
            self.spark.read
            .format("jdbc")
            .option("url", self.jdbc_url)
            .option("dbtable", dbtable)
            .option("user", self.jdbc_user)
            .option("password", self.jdbc_password)
            .option("driver", "org.postgresql.Driver")
            .load()
        )

    def extract(self) -> None:
        """Extrai todos os registros de status de qualidade."""
        logger.info("Extraindo dados de erp_legado.status_qualidade")
        self.df = self._jdbc_read("erp_legado.status_qualidade")

    def transform(self) -> None:
        """Sem transformações adicionais na camada Bronze."""
        pass

    def load(self) -> None:
        """Persiste os dados na camada Bronze em formato Parquet."""
        self.append_parquet()

    def unit_tests(self) -> None:
        """Verifica completude e unicidade dos campos obrigatórios."""
        self.error_checks = (
            self.ErrorCheck
            .isComplete("id")
            .isComplete("nome")
            .isUnique("nome")
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Bronze - PostgreSQL Status de Qualidade")
    parser.add_argument("--execution_date", required=True, help="Data de execução (YYYY-MM-DD)")
    parser.add_argument("--environment", required=True, help="Ambiente de execução (hmg/prd)")
    parser.add_argument("--owner", required=True, help="Responsável pelo job")
    parser.add_argument("--jdbc_url", required=True, help="URL JDBC do PostgreSQL (ex: jdbc:postgresql://host:5432/db)")
    parser.add_argument("--jdbc_user", required=True, help="Usuário do banco de dados")
    parser.add_argument("--jdbc_password", required=True, help="Senha do banco de dados")
    params = vars(parser.parse_args())

    spark = (
        SparkSession.builder
        .appName("bronze-postgres-status-qualidade")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", ""))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", ""))
        .getOrCreate()
    )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    logger.info("Iniciando pipeline Bronze - PostgreSQL Status de Qualidade")
    logger.info(f"Parâmetros de execução: {params}")

    StatusQualidadeETL(
        spark=spark,
        execution_date=params["execution_date"],
        owner=params["owner"],
        environment=params["environment"],
        jdbc_url=params["jdbc_url"],
        jdbc_user=params["jdbc_user"],
        jdbc_password=params["jdbc_password"],
    ).run()
