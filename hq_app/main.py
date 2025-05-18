"""This is the HQ App's main module."""
from util import constants, hive_utils as hu, spark_utils as su


def main() -> None:
    """Application entrypoint."""

    with (su.SparkContextManager("hq_app") as scm):
        consolidated_df = scm.read_received_files()
        scm.logger.info(f"Received {consolidated_df.count()} rows of consolidated data from retailer.")

        hu.use_database(scm.spark, constants.HIVE_DATABASE)
        columns_of_customers = hu.get_table_columns(scm.spark, constants.HIVE_TABLE_CUSTOMERS)
        columns_of_orders = hu.get_table_columns(scm.spark, constants.HIVE_TABLE_ORDERS)
        columns_of_order_items = hu.get_table_columns(scm.spark, constants.HIVE_TABLE_ORDER_ITEMS)

        flattened_df = scm.flatten_df(consolidated_df, columns_of_customers,
                                      columns_of_orders, columns_of_order_items)
        customers_df, orders_df, order_items_df = scm.split_df_into_multiple_dfs(
            flattened_df, columns_of_customers, columns_of_orders, columns_of_order_items)
        scm.logger.info("Split consolidated data into 3 dataframes.")
        scm.logger.info(f"Customers = {customers_df.count()} rows.")
        scm.logger.info(f"Orders = {orders_df.count()} rows.")
        scm.logger.info(f"Order Items = {order_items_df.count()} rows.")

        hu.insert_df_into_table(customers_df, constants.HIVE_TABLE_CUSTOMERS)
        hu.insert_df_into_table(orders_df, constants.HIVE_TABLE_ORDERS)
        hu.insert_df_into_table(order_items_df, constants.HIVE_TABLE_ORDER_ITEMS)
        scm.logger.info("Processing done!  Final record counts:")
        scm.logger.info(f"Table {constants.HIVE_TABLE_CUSTOMERS} = "
                        f"{hu.get_row_count(scm.spark, constants.HIVE_TABLE_CUSTOMERS)} rows.")
        scm.logger.info(f"Table {constants.HIVE_TABLE_ORDERS} = "
                        f"{hu.get_row_count(scm.spark, constants.HIVE_TABLE_ORDERS)} rows.")
        scm.logger.info(f"Table {constants.HIVE_TABLE_ORDER_ITEMS} = "
                        f"{hu.get_row_count(scm.spark, constants.HIVE_TABLE_ORDER_ITEMS)} rows.")


if __name__ == "__main__":  # pragma: no cover
    main()
