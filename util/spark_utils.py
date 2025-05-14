"""This module defines the class SparkUtils()."""
from pyspark import sql
from util import constants, log_utils


class SparkContextManager:
    """This class is a context manager for a PySpark session."""

    def __init__(self, app_name: str):
        self.app_name = app_name
        self.spark: sql.SparkSession = None
        self.logger: log_utils.Log4j = None
        self.customers_df: sql.DataFrame = None
        self.orders_df: sql.DataFrame = None
        self.order_items_df: sql.DataFrame = None

    def __enter__(self):
        """Create a PySpark session."""
        self.spark = (
            sql.SparkSession
                .builder
                .appName(self.app_name)
                .master("local[3]")
                .enableHiveSupport()
                .getOrCreate()
                 )
        self.logger = log_utils.Log4j(self.spark)
        self.logger.info(f"Started SparkSession for {self.app_name} at id {id(self.spark)}")
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        """Close the PySpark session."""
        sid = id(self.spark)
        self.spark.stop()
        self.logger.info(f"Stopped SparkSession for {self.app_name} at id {sid}")

    def read_files(self) -> None:
        """Read the DB table dumps into dataframes."""
        self.customers_df = self.spark.read.json(constants.CUSTOMERS_FOLDER_PATH)
        self.orders_df = self.spark.read.json(constants.ORDERS_FOLDER_PATH)
        self.order_items_df = self.spark.read.json(constants.ORDER_ITEMS_FOLDER_PATH)
        self.logger.info("Successfully created dataframes from JSON files!")
