"""This module contains functions to interact with the Hve database."""
from pyspark import sql


def create_database(spark: sql.SparkSession, name: str) -> None:
    """Create Hive database."""
    spark.sql(f"CREATE DATABASE {name};")


def drop_database(spark: sql.SparkSession, name: str) -> None:
    """Drop Hive database."""
    spark.sql(f"DROP DATABASE IF EXISTS {name} CASCADE;")


def use_database(spark: sql.SparkSession, name: str) -> None:
    """Use Hive database."""
    spark.sql(f"USE {name};")


def create_customers_table(spark: sql.SparkSession, name: str) -> None:
    """Create Hive table for customers."""
    query = f"""
            CREATE TABLE {name} (
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
            """
    spark.sql(query)


def create_orders_table(spark: sql.SparkSession, name: str) -> None:
    """Create Hive table for orders."""
    query = f"""
            CREATE TABLE {name} (
                order_id long,
                order_customer_id long,
                order_date string,
                order_status string
            )   STORED AS TEXTFILE;
            """
    spark.sql(query)


def create_order_items_table(spark: sql.SparkSession, name: str) -> None:
    """Create Hive table for order_items."""
    query = f"""
            CREATE TABLE {name} (
                order_item_id long,
                order_item_order_id long,
                order_item_product_id long,
                order_item_product_price float,
                order_item_quantity int,
                order_item_subtotal float
            )   STORED AS TEXTFILE;
            """
    spark.sql(query)


def get_table_columns(spark: sql.SparkSession, name: str) -> list[str]:
    """Get the columns of the Hive table."""
    return spark.table(name).limit(0).columns
