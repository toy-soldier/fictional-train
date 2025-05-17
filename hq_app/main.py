"""This is the HQ App's main module."""
from util import constants, hive_utils as hu, spark_utils as su


def main() -> None:
    """Application entrypoint."""
    with (su.SparkContextManager("hq_app") as scm):
        consolidated_df = scm.read_received_files()

        hu.use_database(scm.spark, constants.HIVE_DATABASE)
        columns_of_customers = hu.get_table_columns(scm.spark, constants.HIVE_TABLE_CUSTOMERS)
        columns_of_orders = hu.get_table_columns(scm.spark, constants.HIVE_TABLE_ORDERS)
        columns_of_order_items = hu.get_table_columns(scm.spark, constants.HIVE_TABLE_ORDER_ITEMS)

        flattened_df = scm.flatten_df(consolidated_df, columns_of_customers,
                                      columns_of_orders, columns_of_order_items)
        customers_df, orders_df, order_items_df = scm.split_df_into_multiple_dfs(
            flattened_df, columns_of_customers, columns_of_orders, columns_of_order_items)


if __name__ == "__main__":  # pragma: no cover
    main()
