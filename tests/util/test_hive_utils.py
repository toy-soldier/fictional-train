"""This module contains unit tests for hive_utils.py."""
from datetime import datetime
from unittest import TestCase

from util import hive_utils, spark_utils


class TestHiveUtils(TestCase):

    @classmethod
    def setUpClass(cls):
        cls._database_name = f"retailer_{datetime.now().strftime('%Y%m%d')}"
        cls._scm = spark_utils.SparkContextManager("unit_testing")
        cls._scm.__enter__()

    @classmethod
    def tearDownClass(cls):
        cls._scm.spark.sql(f"DROP DATABASE IF EXISTS {cls._database_name} CASCADE;")
        cls._scm.__exit__(exc_type=None, exc_value=None, exc_tb=None)

    def test_database_operations(self):
        spark = self._scm.spark

        hive_utils.drop_database(spark, self._database_name)
        # assert that database is non-existent
        self.assertFalse(spark.catalog.databaseExists(self._database_name))

        hive_utils.create_database(spark, self._database_name)
        # assert that database is created
        self.assertTrue(spark.catalog.databaseExists(self._database_name))

        hive_utils.use_database(spark, self._database_name)
        # assert that we are using the correct database
        self.assertEqual(self._database_name, spark.catalog.currentDatabase())

        hive_utils.create_customers_table(spark, "cust")
        hive_utils.create_orders_table(spark, "ord")
        hive_utils.create_order_items_table(spark, "oi")
        # assert that tables are created
        self.assertListEqual(["cust", "oi", "ord"],
                             sorted([t[0] for t in spark.catalog.listTables()]))

        dummy_json = {"order_id":1,"order_date":"2013-07-25","order_customer_id":11599,"order_status":"CLOSED"}
        dummy_schema = "order_id long, order_customer_id long, order_date string, order_status string"
        dummy_df = spark.createDataFrame([dummy_json], dummy_schema)

        # assert that the table columns match the dataframe columns
        self.assertListEqual(sorted(dummy_df.columns),
                             sorted(hive_utils.get_table_columns(spark, "ord")))

        hive_utils.insert_df_into_table(dummy_df, "ord")
        # assert that there's only one record in the table
        self.assertEqual(1, hive_utils.get_row_count(spark, "ord"))
        record = spark.table("ord").selectExpr("*").collect()[0].asDict()
        # assert that the record in the table matches the original json
        self.assertDictEqual(dummy_json, record)

        hive_utils.drop_database(spark, self._database_name)
        # assert that database is destroyed
        self.assertFalse(spark.catalog.databaseExists(self._database_name))
