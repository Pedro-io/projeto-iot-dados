from abc import abstractmethod

from pyspark.sql import DataFrame

from processamento.abstract_etl import AbstractETL
from utils.logger import logger


class SilverETL(AbstractETL):
    """Base class para ETLs da camada Silver.

    Lê Parquet (ou NDJSON) da camada Bronze e escreve Delta na Silver.
    Subclasses implementam tipagem, deduplicação e checks específicos.
    """

    @property
    def bronze_bucket(self) -> str:
        return f"bronze-hmg" if self.environment == "hmg" else "bronze"

    def _read_bronze_parquet(self, source_path: str) -> DataFrame:
        path = f"s3a://{self.bronze_bucket}/{source_path}"
        logger.info(f"Lendo bronze Parquet: {path}")
        return self.spark.read.parquet(path)

    def _read_bronze_ndjson(self, source_path: str) -> DataFrame:
        """Lê NDJSON com descoberta automática de partições Hive."""
        path = f"s3a://{self.bronze_bucket}/{source_path}"
        logger.info(f"Lendo bronze NDJSON: {path}")
        return (
            self.spark.read
            .option("basePath", path)
            .json(path)
        )

    def _deduplicate(self, keys: list) -> None:
        before = self.df.count()
        self.df = self.df.dropDuplicates(keys)
        after = self.df.count()
        removed = before - after
        if removed > 0:
            logger.warning(f"Deduplicação: {removed} registro(s) removido(s) por {keys}.")

    @abstractmethod
    def extract(self) -> None:
        pass

    @abstractmethod
    def transform(self) -> None:
        pass

    @abstractmethod
    def load(self) -> None:
        pass

    @abstractmethod
    def unit_tests(self) -> None:
        pass
