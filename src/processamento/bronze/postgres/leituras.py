import argparse
import os

from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession

from processamento.abstract_etl import AbstractETL
from utils.logger import logger


class LeiturasETL(AbstractETL):
    """ETL para extração incremental da tabela transacional `leituras` do PostgreSQL.

    Carga incremental com janela deslizante baseada na data de execução:
    - Até o dia 10: janela começa no 1º do mês anterior (dados atrasados ainda chegam).
    - Após o dia 10: janela começa no 1º do mês corrente.

    A tabela armazena todas as leituras dos sensores com valor, status de qualidade
    e flag de anomalia.
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
            table_path="bronze/postgres/leituras",
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
        """Extrai leituras do período definido pela janela deslizante."""
        execution_dt = self._parse_execution_date()
        start_date = execution_dt.replace(day=1)
        if execution_dt.day <= 10:
            start_date = (execution_dt - relativedelta(months=1)).replace(day=1)

        logger.info(f"Janela de extração: {start_date} até {execution_dt}")

        query = (
            f"(SELECT * FROM erp_legado.leituras "
            f"WHERE DATE(timestamp_leitura) BETWEEN '{start_date}' AND '{execution_dt}'"
            f") AS leituras_filtradas"
        )
        self.df = self._jdbc_read(query)

    def transform(self) -> None:
        """Sem transformações adicionais na camada Bronze."""
        pass

    def load(self) -> None:
        """Persiste os dados na camada Bronze em formato Parquet particionado por data."""
        self.append_parquet(partition_by=["_execution_date"])

    def unit_tests(self) -> None:
        """Verifica completude e consistência das leituras extraídas."""
        self.error_checks = (
            self.ErrorCheck
            .isComplete("id")
            .isComplete("sensor_id")
            .isComplete("valor")
            .isComplete("status_qualidade_id")
            .isComplete("timestamp_leitura")
            .isNonNegative("valor")
        )
        self.warning_checks = (
            self.WarningCheck
            .isComplete("is_anomalia")
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Bronze - PostgreSQL Leituras de Sensores")
    parser.add_argument("--execution_date", required=True, help="Data de execução (YYYY-MM-DD)")
    parser.add_argument("--environment", required=True, help="Ambiente de execução (hmg/prd)")
    parser.add_argument("--owner", required=True, help="Responsável pelo job")
    parser.add_argument("--jdbc_url", required=True, help="URL JDBC do PostgreSQL (ex: jdbc:postgresql://host:5432/db)")
    parser.add_argument("--jdbc_user", required=True, help="Usuário do banco de dados")
    parser.add_argument("--jdbc_password", required=True, help="Senha do banco de dados")
    params = vars(parser.parse_args())

    spark = (
        SparkSession.builder
        .appName("bronze-postgres-leituras")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", ""))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", ""))
        .getOrCreate()
    )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    logger.info("Iniciando pipeline Bronze - PostgreSQL Leituras de Sensores")
    logger.info(f"Parâmetros de execução: {params}")

    LeiturasETL(
        spark=spark,
        execution_date=params["execution_date"],
        owner=params["owner"],
        environment=params["environment"],
        jdbc_url=params["jdbc_url"],
        jdbc_user=params["jdbc_user"],
        jdbc_password=params["jdbc_password"],
    ).run()
