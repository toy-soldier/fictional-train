"""This module defines the class Log4j()."""
import logging
from pyspark import sql


class Log4j:
    """
    Wrapper around py4j, to allow the application to use Java's log4j for logging.
    Note that the log4j configuration doesn't work, so the wrapper creates s Python logger instead.
    """
    def __init__(self, spark: sql.SparkSession) -> None:
        root_class = "guru.learningjournal.spark.examples"
        conf = spark.sparkContext.getConf()
        app_name = conf.get("spark.app.name")

        # log4j = spark._jvm.org.apache.log4j
        # self.logger = log4j.LogManager.getLogger(root_class + "." + app_name)

        self.logger = self.setup_python_logger_instead(root_class, app_name)

    def warning(self, message: str) -> None:
        """Invoke warning() on the wrapped logger."""
        self.logger.warning(message)

    def info(self, message: str) -> None:
        """Invoke info() on the wrapped logger."""
        self.logger.info(message)

    def error(self, message: str) -> None:
        """Invoke error() on the wrapped logger."""
        self.logger.error(message)

    def debug(self, message: str) -> None:
        """Invoke debug() on the wrapped logger."""
        self.logger.debug(message)

    def setup_python_logger_instead(self, root_class: str, app_name: str) -> logging.Logger:
        """Setup a python logger."""
        logger = logging.getLogger(root_class + "." + app_name)
        logger.setLevel(logging.DEBUG)

        console_handler = logging.StreamHandler()
        file_handler = logging.FileHandler(
            f"{app_name}.log",
            mode="w",
            encoding="utf-8"
        )
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
        formatter = logging.Formatter(
            "{asctime} - {levelname} - {message}",
            style="{",                  # use str.format() to write the log's timestamp
            datefmt="%Y-%m-%d %H:%M"    # format of the log's timestamp
        )

        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)
        console_handler.setLevel(logging.INFO)
        file_handler.setLevel(logging.INFO)
        return logger
