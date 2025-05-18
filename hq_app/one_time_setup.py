"""This module contains a script for one time database setup."""
from util import constants, hive_utils as hu, spark_utils as su


def main() -> None:
    """Script entrypoint."""
    with su.SparkContextManager("one_time_setup") as scm:
        hu.drop_database(scm.spark, constants.HIVE_DATABASE)
        hu.create_database(scm.spark, constants.HIVE_DATABASE)
        hu.use_database(scm.spark, constants.HIVE_DATABASE)
        hu.create_customers_table(scm.spark, constants.HIVE_TABLE_CUSTOMERS)
        hu.create_orders_table(scm.spark, constants.HIVE_TABLE_ORDERS)
        hu.create_order_items_table(scm.spark, constants.HIVE_TABLE_ORDER_ITEMS)


if __name__ == "__main__":  # pragma: no cover
    main()
