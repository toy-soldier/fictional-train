"""This module contains unit tests for spark_utils.py."""
import tempfile
from unittest import TestCase, mock

from util import constants, spark_utils

customer_record_1 = {"customer_id": 100, "customer_name": "Customer 1"}
order_record_11 = {"order_id": 42008, "order_customer_id": 100}
order_record_12 = {"order_id": 52008, "order_customer_id": 100}
order_item_record_111 = {"order_item_id": 95546, "order_item_order_id": 42008}
order_item_record_112 = {"order_item_id": 85546, "order_item_order_id": 42008}
order_item_record_121 = {"order_item_id": 85546, "order_item_order_id": 52008}
customer_record_2 = {"customer_id": 200, "customer_name": "Customer 2"}
order_record_21 = {"order_id": 742, "order_customer_id": 200}
order_item_record_211 = {"order_item_id": 116, "order_item_order_id": 742}

class TestSparkContextManager(TestCase):

    @classmethod
    def setUpClass(cls):
        cls._scm = spark_utils.SparkContextManager("unit_testing")
        cls._scm.__enter__()

        cls._customers_df = cls._scm.spark.createDataFrame([customer_record_1])
        cls._orders_df = cls._scm.spark.createDataFrame([order_record_11])
        cls._order_items_df = cls._scm.spark.createDataFrame([order_item_record_111])
        cls._consolidated_df = cls._scm.spark.read.json(constants.TEST_DATA_FOLDER_PATH, multiLine=True)

        cls._columns_of_customers = cls._customers_df.columns
        cls._columns_of_orders = cls._orders_df.columns
        cls._columns_of_order_items = cls._order_items_df.columns

    @classmethod
    def tearDownClass(cls):
        cls._scm.__exit__(exc_type=None, exc_value=None, exc_tb=None)

    @mock.patch.object(spark_utils, "constants")
    def test_read_files(self, mocked_constants):
        self._scm.logger.debug("Testing spark_utils.read_files()")
        with (
            tempfile.TemporaryDirectory() as dummy_customers_folder,
            tempfile.TemporaryDirectory() as dummy_orders_folder,
            tempfile.TemporaryDirectory() as dummy_order_items_folder,
        ):
            self._scm.logger.debug("Writing dummy files into dummy folders...")

            self._customers_df.write.mode("overwrite").json(dummy_customers_folder)
            self._orders_df.write.mode("overwrite").json(dummy_orders_folder)
            self._order_items_df.write.mode("overwrite").json(dummy_order_items_folder)

            mocked_constants.CUSTOMERS_FOLDER_PATH = dummy_customers_folder
            mocked_constants.ORDERS_FOLDER_PATH = dummy_orders_folder
            mocked_constants.ORDER_ITEMS_FOLDER_PATH = dummy_order_items_folder

            customers_df, orders_df, order_items_df = self._scm.read_files()

            self.assertDictEqual(
                customer_record_1,
                customers_df.collect()[0].asDict(),
                "Customers DF not read properly"
            )
            self.assertDictEqual(
                order_record_11,
                orders_df.collect()[0].asDict(),
                "Orders DF not read properly"
            )
            self.assertDictEqual(
                order_item_record_111,
                order_items_df.collect()[0].asDict(),
                "Order Items DF not read properly"
            )

        self._scm.logger.debug("Test passed!")

    def test_join_dataframes(self):
        self._scm.logger.debug("Testing spark_utils.join_dataframes()")
        customer_orders_df = self._scm.join_dataframes(self._customers_df,
                                                       self._orders_df, self._order_items_df)

        self.assertEqual(1, customer_orders_df.count())

        self.assertDictEqual(
            {**customer_record_1, **order_record_11, **order_item_record_111},
            customer_orders_df.collect()[0].asDict(),
            "Customer Orders DF not created properly"
        )
        self._scm.logger.debug("Test passed!")

    def test_form_order_items_by_orders_df(self):
        self._scm.logger.debug("Testing spark_utils.form_order_items_by_orders_df()")
        order_items_df = self._scm.spark.createDataFrame([order_item_record_111, order_item_record_112])
        customer_orders_df = self._scm.join_dataframes(self._customers_df,
                                                       self._orders_df, order_items_df)

        order_items_by_orders_df = self._scm.form_order_items_by_orders_df(customer_orders_df,
                                                                           self._columns_of_customers,
                                                                           self._columns_of_orders,
                                                                           self._columns_of_order_items)
        self.assertEqual(1, order_items_by_orders_df.count())

        self.assertDictEqual(
            {**customer_record_1, **order_record_11, "order_items": [order_item_record_112, order_item_record_111]},
            order_items_by_orders_df.collect()[0].asDict(recursive=True),
            "Consolidated DF not created properly"
        )
        self._scm.logger.debug("Test passed!")

    def test_form_orders_of_same_customer_df(self):
        self._scm.logger.debug("Testing spark_utils.form_orders_of_same_customer_df()")
        orders_df = self._scm.spark.createDataFrame([order_record_11, order_record_12])
        order_items_df = self._scm.spark.createDataFrame([order_item_record_111, order_item_record_121])
        customer_orders_df = self._scm.join_dataframes(self._customers_df,
                                                       orders_df, order_items_df)
        order_items_by_orders_df = self._scm.form_order_items_by_orders_df(customer_orders_df,
                                                                           self._columns_of_customers,
                                                                           self._columns_of_orders,
                                                                           self._columns_of_order_items)
        orders_of_same_customer_df = self._scm.form_orders_of_same_customer_df(order_items_by_orders_df,
                                                                           self._columns_of_customers,
                                                                           self._columns_of_orders)
        self.assertEqual(1, orders_of_same_customer_df.count())
        self.assertDictEqual(
            {**customer_record_1,
                "orders": [
                    {**order_record_11, "order_items": [order_item_record_111]},
                    {**order_record_12, "order_items": [order_item_record_121]}
                ]},
            orders_of_same_customer_df.collect()[0].asDict(recursive=True),
            "Consolidated DF not created properly"
        )
        self._scm.logger.debug("Test passed!")

    @mock.patch.object(spark_utils, "constants")
    def test_write_df(self, mocked_constants):
        self._scm.logger.debug("Testing spark_utils.write_df()")

        self._scm.logger.debug("Creating dummy dataframe...")
        customers_df = self._scm.spark.createDataFrame([customer_record_1, customer_record_2])
        orders_df = self._scm.spark.createDataFrame([order_record_11, order_record_12, order_record_21])
        order_items_df = self._scm.spark.createDataFrame(
            [order_item_record_111, order_item_record_112, order_item_record_121, order_item_record_211])
        customer_orders_df = self._scm.join_dataframes(customers_df, orders_df, order_items_df)

        order_items_by_orders_df = self._scm.form_order_items_by_orders_df(customer_orders_df,
                                                                           self._columns_of_customers,
                                                                           self._columns_of_orders,
                                                                           self._columns_of_order_items)
        orders_of_same_customer_df = self._scm.form_orders_of_same_customer_df(order_items_by_orders_df,
                                                                           self._columns_of_customers,
                                                                           self._columns_of_orders)
        expected = [row.asDict(recursive=True) for row in orders_of_same_customer_df.collect()]

        with (tempfile.TemporaryDirectory() as dummy_output_folder):
            self._scm.logger.debug("Writing dummy dataframe into dummy folder...")
            mocked_constants.OUTPUT_FOLDER_PATH = dummy_output_folder
            self._scm.write_df(orders_of_same_customer_df)

            saved_df = self._scm.spark.read.parquet(dummy_output_folder)
            actual = [row.asDict(recursive=True) for row in saved_df.collect()]
            self.assertListEqual(expected, actual, "Saved DF not read correctly!")

        self._scm.logger.debug("Test passed!")

    @mock.patch.object(spark_utils, "constants")
    def test_read_received_files(self, mocked_constants):
        self._scm.logger.debug("Testing spark_utils.read_received_files()")

        expected = [row.asDict(recursive=True) for row in self._consolidated_df.collect()]
        with (tempfile.TemporaryDirectory() as dummy_output_folder):
            self._scm.logger.debug("Writing dummy dataframe into dummy folder...")
            self._consolidated_df.write.mode("overwrite").parquet(dummy_output_folder)

            mocked_constants.OUTPUT_FOLDER_PATH = dummy_output_folder

            saved_df = self._scm.read_received_files()
            actual = [row.asDict(recursive=True) for row in saved_df.collect()]
            self.assertListEqual(expected, actual, "Saved DF not read correctly!")

        self._scm.logger.debug("Test passed!")
