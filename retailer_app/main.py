"""This is the Retailer App's main module."""
from util import spark_utils as su


def main() -> None:
    """Application entrypoint."""
    with (su.SparkContextManager("retailer_app") as scm):
        customers_df, orders_df, order_items_df = scm.read_files()
        scm.logger.info("Read table exports into 3 dataframes.")
        scm.logger.info(f"Customers = {customers_df.count()} rows.")
        scm.logger.info(f"Orders = {orders_df.count()} rows.")
        scm.logger.info(f"Order Items = {order_items_df.count()} rows.")

        columns_of_customers = customers_df.columns
        columns_of_orders = orders_df.columns
        columns_of_order_items = order_items_df.columns

        customer_orders_df = scm.join_dataframes(customers_df, orders_df, order_items_df)

        order_items_of_same_order_df = scm.form_order_items_by_orders_df(
            customer_orders_df, columns_of_customers, columns_of_orders, columns_of_order_items)
        orders_of_same_customer_df = scm.form_orders_of_same_customer_df(
            order_items_of_same_order_df,columns_of_customers, columns_of_orders)
        scm.write_df(orders_of_same_customer_df)
        scm.logger.info(f"To send {orders_of_same_customer_df.count()} rows of consolidated data to HQ.")


if __name__ == "__main__":  # pragma: no cover
    main()
