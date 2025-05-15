"""This module contains unit tests for spark_utils.py."""
import tempfile
from unittest import TestCase, mock

from util import spark_utils

customer_record = {"customer_id": 100, "customer_name": "Customer 1"}
order_record = {"order_id": 42008, "order_customer_id": 100}
order_record_2 = {"order_id": 52008, "order_customer_id": 100}
order_item_record = {"order_item_id": 95546, "order_item_order_id": 42008}
order_item_record_2 = {"order_item_id": 85546, "order_item_order_id": 42008}
order_item_record_3 = {"order_item_id": 85546, "order_item_order_id": 52008}


class TestSparkContextManager(TestCase):

    @classmethod
    def setUpClass(cls):
        cls._scm = spark_utils.SparkContextManager("unit_testing")
        cls._scm.__enter__()

        cls._customers_df = cls._scm.spark.createDataFrame([customer_record])
        cls._orders_df = cls._scm.spark.createDataFrame([order_record])
        cls._order_items_df = cls._scm.spark.createDataFrame([order_item_record])

        cls._columns_of_customers = cls._customers_df.columns
        cls._columns_of_orders = cls._orders_df.columns
        cls._columns_of_order_items = cls._order_items_df.columns

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

            self._customers_df.write.mode("overwrite").format("json").save(dummy_customers_folder)
            self._orders_df.write.mode("overwrite").format("json").save(dummy_orders_folder)
            self._order_items_df.write.mode("overwrite").format("json").save(dummy_order_items_folder)

            mocked_constants.CUSTOMERS_FOLDER_PATH = dummy_customers_folder
            mocked_constants.ORDERS_FOLDER_PATH = dummy_orders_folder
            mocked_constants.ORDER_ITEMS_FOLDER_PATH = dummy_order_items_folder

            customers_df, orders_df, order_items_df = self._scm.read_files()

            self.assertDictEqual(
                customer_record,
                customers_df.collect()[0].asDict(),
                "Customers DF not read properly"
            )
            self.assertDictEqual(
                order_record,
                orders_df.collect()[0].asDict(),
                "Orders DF not read properly"
            )
            self.assertDictEqual(
                order_item_record,
                order_items_df.collect()[0].asDict(),
                "Order Items DF not read properly"
            )

        self._scm.logger.info("Test passed!")

    def test_join_dataframes(self):
        self._scm.logger.info("Testing spark_utils.join_dataframes()")
        customer_orders_df = self._scm.join_dataframes(self._customers_df,
                                                       self._orders_df, self._order_items_df)

        self.assertEqual(1, customer_orders_df.count())

        self.assertDictEqual(
            {**customer_record, **order_record, **order_item_record},
            customer_orders_df.collect()[0].asDict(),
            "Customer Orders DF not created properly"
        )
        self._scm.logger.info("Test passed!")

    def test_form_order_items_by_orders_df(self):
        self._scm.logger.info("Testing spark_utils.form_order_items_by_orders_df()")
        order_items_df = self._scm.spark.createDataFrame([order_item_record, order_item_record_2])
        customer_orders_df = self._scm.join_dataframes(self._customers_df,
                                                       self._orders_df, order_items_df)

        order_items_by_orders_df = self._scm.form_order_items_by_orders_df(customer_orders_df,
                                                                           self._columns_of_customers,
                                                                           self._columns_of_orders,
                                                                           self._columns_of_order_items)
        self.assertEqual(1, order_items_by_orders_df.count())

        self.assertDictEqual(
            {**customer_record, **order_record, "order_items": [order_item_record, order_item_record_2]},
            order_items_by_orders_df.collect()[0].asDict(recursive=True),
            "Consolidated DF not created properly"
        )
        self._scm.logger.info("Test passed!")

    def test_form_orders_of_same_customer_df(self):
        self._scm.logger.info("Testing spark_utils.form_orders_of_same_customer_df()")
        orders_df = self._scm.spark.createDataFrame([order_record, order_record_2])
        order_items_df = self._scm.spark.createDataFrame([order_item_record, order_item_record_3])
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
            {**customer_record,
                "orders": [
                    {**order_record, "order_items": [order_item_record]},
                    {**order_record_2, "order_items": [order_item_record_3]}
                ]},
            orders_of_same_customer_df.collect()[0].asDict(recursive=True),
            "Consolidated DF not created properly"
        )
        self._scm.logger.info("Test passed!")
