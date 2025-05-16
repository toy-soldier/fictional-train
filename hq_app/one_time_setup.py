"""
This module contains a script for one time database setup.
WARNING: This script should be executed only once!!!
"""
from util import spark_utils as su


def main() -> None:
    """Script entrypoint."""
    with su.SparkContextManager("one_time_setup") as scm:
        scm.spark.sql("DROP DATABASE IF EXISTS retailer CASCADE;")
        scm.spark.sql("CREATE DATABASE retailer;")
        scm.spark.sql("USE retailer;")
        create_table_scripts = [
            """
            CREATE TABLE customers (
                customer_id long,
                customer_fname string,
                customer_lname string,
                customer_email string,
                customer_password string,
                customer_street string,
                customer_city string,
                customer_state string,
                customer_zipcode string
            )   STORED AS TEXTFILE;
            """,
            """
            CREATE TABLE orders (
                order_id long,
                order_customer_id long,
                order_date string,
                order_status string
            )   STORED AS TEXTFILE;
            """,
            """
            CREATE TABLE order_items (
                order_item_id long,
                order_item_order_id long,
                order_item_product_id long,
                order_item_product_price float,
                order_item_quantity int,
                order_item_subtotal float
            )   STORED AS TEXTFILE;
            """,
        ]
        for script in create_table_scripts:
            scm.spark.sql(script)


if __name__ == "__main__":
    main()
