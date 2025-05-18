"""
This module simply calls main() in one_time_setup.py;
validation of whether the script works is done manually.
"""
from datetime import datetime
from unittest import TestCase, mock
from hq_app import one_time_setup


class TestOneTimeSetup(TestCase):

    @mock.patch.object(one_time_setup, "constants")
    def test_main(self, mocked_constants):
        database_name = f"retailer_{datetime.now().strftime('%Y%m%d')}"
        mocked_constants.HIVE_DATABASE = database_name
        mocked_constants.HIVE_TABLE_ORDERS = "orders"
        mocked_constants.HIVE_TABLE_CUSTOMERS = "customers"
        mocked_constants.HIVE_TABLE_ORDER_ITEMS = "order_items"

        one_time_setup.main()
