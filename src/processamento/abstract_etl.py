import datetime as dt
import os
from abc import ABC, abstractmethod
from time import sleep
from typing import Optional

# pydeequ lê SPARK_VERSION no import; precisa ser definido antes
os.environ.setdefault("SPARK_VERSION", "3.5")

from delta.exceptions import ConcurrentAppendException
from delta.tables import DeltaTable
from pydeequ.checks import Check, CheckLevel, ConstrainableDataTypes
from pydeequ.verification import VerificationResult, VerificationSuite
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.utils import AnalysisException

from utils.logger import logger


class AbstractETL(ABC):
    def __init__(
        self,
        spark: SparkSession,
        table_path: str,
        execution_date: str,
        owner: str,
        environment: str = "hmg",
        **kwargs,
    ):
        self.spark = spark
        self.execution_date = execution_date
        self.owner = owner
        self.environment = self._check_environment(environment)

        self.layer_name, self.source_name, self.table_name = (
            self._get_destination_layers(table_path)
        )

        self.destination_bucket = (
            f"{self.layer_name}-hmg" if self.environment == "hmg" else self.layer_name
        )

    @property
    def destination_path(self) -> str:
        return f"{self.source_name}/{self.table_name}"

    def _check_environment(self, environment: str) -> str:
        valid = {"hmg", "prd"}
        if environment in valid:
            return environment
        raise ValueError(
            f"Invalid environment: {environment!r} (options: {sorted(valid)})"
        )

    def _get_destination_layers(self, table_path: str) -> tuple[str, str, str]:
        sliced_path = table_path.strip("/").split("/")
        return sliced_path[-3], sliced_path[-2], sliced_path[-1]

    def _parse_execution_date(self) -> dt.date:
        return dt.datetime.strptime(self.execution_date, "%Y-%m-%d").date()

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

    def run_unit_tests(self) -> None:

        self.ErrorCheck = Check(self.spark, CheckLevel.Error, "Error Check")
        self.WarningCheck = Check(self.spark, CheckLevel.Warning, "Warning Check")
        self.ConstrainableDataTypes = ConstrainableDataTypes
        self.unit_tests()

        checks_df = None
        if hasattr(self, "error_checks") and hasattr(self, "warning_checks"):
            checks_df = self._run_unit_tests_per_level(self.error_checks).union(
                self._run_unit_tests_per_level(self.warning_checks)
            )
        elif hasattr(self, "error_checks"):
            checks_df = self._run_unit_tests_per_level(self.error_checks)
        elif hasattr(self, "warning_checks"):
            checks_df = self._run_unit_tests_per_level(self.warning_checks)

        if checks_df is not None:
            checks_df = (
                checks_df
                .withColumn("_execution_date", lit(self._parse_execution_date()))
                .withColumn("_processed_at", current_timestamp())
            )

            failures_df = checks_df.filter(
                checks_df.constraint_status != "Success"
            ).select("constraint", "check_level", "constraint_message")

            if failures_df.count() > 0:
                logger.warning("Failed unit tests:")
                failures_df.show(truncate=False)

                if failures_df.filter(failures_df.check_level == "Error").count() > 0:
                    raise RuntimeError("Unit tests failed.")
        else:
            logger.warning("Unit tests not found.")

    def _run_unit_tests_per_level(self, checks) -> Optional[DataFrame]:
        if checks:
            result = (
                VerificationSuite(self.spark).onData(self.df).addCheck(checks).run()
            )
            return VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return None

    def add_metadata_columns(self) -> None:
        if "_execution_date" not in self.df.columns:
            self.df = self.df.withColumn(
                "_execution_date", lit(self._parse_execution_date())
            )

        self.df = self.df.withColumn("_processed_at", current_timestamp())

        if "_data_source" not in self.df.columns:
            data_source = self.source_name
            self.df = self.df.withColumn("_data_source", lit(data_source))

    def run(self) -> None:
        try:
            logger.info(f"Environment: {self.environment}")
            logger.info("Starting ETL process.")

            logger.info("Extracting data.")
            self.extract()
            logger.info(f"Batch size: {self.df.count()}")

            logger.info("Transforming data.")
            self.transform()
            if self.df.count() == 0:
                logger.warning("No transformed data! Aborting the ETL process...")
                return

            logger.info("Adding metadata columns.")
            self.add_metadata_columns()

            logger.info("Running unit tests.")
            self.run_unit_tests()

            logger.info("Loading data.")
            self.load()

        except Exception as exc:
            self.spark.sparkContext._gateway.shutdown_callback_server()
            self.spark.stop()
            raise RuntimeError("ETL process failed.") from exc

        logger.info("End.")
        self.spark.sparkContext._gateway.shutdown_callback_server()
        self.spark.stop()

    def upsert_delta_table(
        self,
        unique_key_condition: str,
        partition_by: Optional[list] = None,
        previous_delete: bool = True,
        use_row_hash: bool = False,
    ) -> None:
        path = f"s3a://{self.destination_bucket}/{self.destination_path}"

        try:
            delta_table = DeltaTable.forPath(self.spark, path)
        except AnalysisException:
            delta_table = None

        if delta_table:
            merge_condition = (
                "(source._execution_date >= target._execution_date) "
                "AND (source._row_hash != target._row_hash)"
                if use_row_hash
                else "source._execution_date >= target._execution_date"
            )

            for attempt in range(5):
                try:
                    if previous_delete:
                        logger.info(f"Deleting _execution_date {self.execution_date}...")
                        delta_table.delete(col("_execution_date") == self.execution_date)

                    suffix = " (with row hash check)" if use_row_hash else ""
                    logger.info(f"Upserting into the delta table{suffix}...")
                    (
                        delta_table.alias("target")
                        .merge(self.df.alias("source"), unique_key_condition)
                        .whenMatchedUpdateAll(condition=merge_condition)
                        .whenNotMatchedInsertAll()
                        .execute()
                    )
                    break

                except ConcurrentAppendException:
                    logger.warning(
                        f"Failed to upsert concurrent batch, attempt {attempt}"
                    )
                    if attempt == 4:
                        raise RuntimeError(
                            f"Failed to upsert concurrent batch {attempt + 1} times."
                        )
                    sleep(60)
        else:
            save_params: dict = {"format": "delta", "mode": "overwrite"}
            if partition_by:
                save_params["partitionBy"] = partition_by

            logger.info("Delta table does not exist, creating...")
            self.df.write.save(path, **save_params)

        logger.info("Successfully logged new data into delta table!")

    def append_parquet(self, partition_by: Optional[list] = None) -> None:
        path = f"s3a://{self.destination_bucket}/{self.destination_path}"

        save_params: dict = {"format": "parquet", "mode": "append"}
        if partition_by:
            save_params["partitionBy"] = partition_by

        logger.info(f"Appending parquet data to {path}...")
        self.df.write.save(path, **save_params)
        logger.info("Successfully appended parquet data!")

