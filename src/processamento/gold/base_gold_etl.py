from abc import abstractmethod

from pyspark.sql import DataFrame

from processamento.abstract_etl import AbstractETL
from utils.logger import logger


class GoldETL(AbstractETL):
    """Base class para ETLs da camada Gold.

    Lê Delta da camada Silver e escreve Delta na Gold.
    Subclasses implementam joins, agregações e checks específicos.
    """

    @property
    def silver_bucket(self) -> str:
        return "silver-hmg" if self.environment == "hmg" else "silver"

    def _read_silver_delta(self, source_path: str) -> DataFrame:
        path = f"s3a://{self.silver_bucket}/{source_path}"
        logger.info(f"Lendo Silver Delta: {path}")
        return self.spark.read.format("delta").load(path)

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
