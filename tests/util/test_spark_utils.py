"""This module contains unit tests for spark_utils.py."""
import tempfile
from unittest import TestCase, mock
from util import spark_utils


class TestSparkContextManager(TestCase):

    @classmethod
    def setUpClass(cls):
        cls._scm = spark_utils.SparkContextManager("unit_testing")
        cls._scm.__enter__()

    @classmethod
    def tearDownClass(cls):
        cls._scm.__exit__(exc_type=None, exc_value=None, exc_tb=None)

    @mock.patch.object(spark_utils, "constants")
    def test_read_files(self, mocked_constants):
        self._scm.logger.info("Testing spark_utils.read_files()")
        with (
            tempfile.TemporaryDirectory() as dummy_customers_folder,
            tempfile.TemporaryDirectory() as dummy_orders_folder,
            tempfile.TemporaryDirectory() as dummy_order_items_folder,
        ):
            self._scm.logger.info("Writing dummy files into dummy folders...")
            self._scm.spark.createDataFrame(
                [{"id": 100, "name": "Customer 1"}]
            ).write.mode("overwrite").format("json").save(dummy_customers_folder)
            self._scm.spark.createDataFrame(
                [{"id": 42008, "order_customer_id": 100}]
            ).write.mode("overwrite").format("json").save(dummy_orders_folder)
            self._scm.spark.createDataFrame(
                [{"id": 95546, "order_item_order_id": 42008}]
            ).write.mode("overwrite").format("json").save(dummy_order_items_folder)

            mocked_constants.CUSTOMERS_FOLDER_PATH = dummy_customers_folder
            mocked_constants.ORDERS_FOLDER_PATH = dummy_orders_folder
            mocked_constants.ORDER_ITEMS_FOLDER_PATH = dummy_order_items_folder

            self._scm.read_files()

            self.assertDictEqual(
                {"id": 100, "name": "Customer 1"},
                self._scm.customers_df.collect()[0].asDict(),
                "Customers DF not read properly"
            )
            self.assertDictEqual(
                {"id": 42008, "order_customer_id": 100},
                self._scm.orders_df.collect()[0].asDict(),
                "Orders DF not read properly"
            )
            self.assertDictEqual(
                {"id": 95546, "order_item_order_id": 42008},
                self._scm.order_items_df.collect()[0].asDict(),
                "Order Items DF not read properly"
            )

        self._scm.logger.info("Test passed!")
