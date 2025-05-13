"""This module defines the class SparkUtils()."""
from pyspark import sql


class SparkContextManager:
    """This class is a context manager for a PySpark session."""

    def __init__(self, app_name: str):
        self.app_name = app_name
        self.spark = None

    def __enter__(self) -> sql.SparkSession:
        """Create a PySpark session."""
        self.spark = (
            sql.SparkSession
                .builder
                .appName(self.app_name)
                .master("local[3]")
                .enableHiveSupport()
                .getOrCreate()
                 )
        return self.spark

    def __exit__(self, exc_type, exc_value, exc_tb):
        """Close the PySpark session."""
        self.spark.stop()

