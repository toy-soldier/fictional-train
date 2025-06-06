"""This module defines the class SparkUtils()."""
from pyspark import sql
from pyspark.sql import functions as func
from util import constants, log_utils


class SparkContextManager:
    """This class is a context manager for a PySpark session."""

    def __init__(self, app_name: str):
        self.app_name = app_name
        self.spark: sql.SparkSession = None
        self.logger: log_utils.Log4j = None

    def __enter__(self):
        """Create a PySpark session."""
        self.spark = (
            sql.SparkSession
                .builder
                .appName(self.app_name)
                .master(constants.SPARK_MASTER)
                .config("spark.sql.warehouse.dir", constants.SPARK_WAREHOUSE_DIR)
                .enableHiveSupport()
                .getOrCreate()
                 )
        self.logger = log_utils.Log4j(self.spark)
        self.logger.info(f"Started SparkSession for {self.app_name} at id {id(self.spark)}")
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        """Close the PySpark session."""
        sid = id(self.spark)
        self.spark.stop()
        self.logger.info(f"Stopped SparkSession for {self.app_name} at id {sid}")

    def read_files(self) -> tuple[sql.DataFrame, sql.DataFrame, sql.DataFrame]:
        """Read the DB table dumps into dataframes."""
        self.logger.info("Now reading files into dataframes...")
        customers_df = self.spark.read.json(constants.CUSTOMERS_FOLDER_PATH)
        orders_df = self.spark.read.json(constants.ORDERS_FOLDER_PATH)
        order_items_df = self.spark.read.json(constants.ORDER_ITEMS_FOLDER_PATH)
        self.logger.info("Successfully created dataframes from JSON files!")
        return customers_df, orders_df, order_items_df

    def join_dataframes(self, customers_df: sql.DataFrame,
                        orders_df: sql.DataFrame, order_items_df: sql.DataFrame) -> sql.DataFrame:
        """Create the `customer_orders_df` by joining the 3 dataframes."""
        self.logger.info("Now joining the three dataframes into `customer_orders_df`...")

        # not all customers have order_id in orders table
        # not all orders have order_item_id in order_items table

        customer_orders_df = customers_df. \
            join(
                orders_df,
                customers_df["customer_id"] == orders_df["order_customer_id"],
                how="left"
            ). \
            join(
                order_items_df,
                orders_df["order_id"] == order_items_df["order_item_order_id"],
                how="left"
            )
        self.logger.info("Joined dataframes into `customer_orders_df`!")
        return customer_orders_df

    def form_order_items_by_orders_df(self, customer_orders_df: sql.DataFrame,
                                      columns_of_customers: list[str],
                                      columns_of_orders: list[str],
                                      columns_of_order_items: list[str]) -> sql.DataFrame:
        """Consolidate order items by orders."""
        self.logger.info("Consolidating the order items within each order...")
        order_items_of_same_order_df = customer_orders_df. \
            select(
                *columns_of_customers, *columns_of_orders,
                func.struct(*columns_of_order_items).alias("order_items_structs")
            ). \
            groupBy(
                *columns_of_customers, *columns_of_orders
            ). \
            agg(func.collect_list("order_items_structs").alias("order_items")). \
            orderBy("customer_id", "order_id")
        self.logger.info("Consolidation finished!")
        return order_items_of_same_order_df

    def form_orders_of_same_customer_df(self, order_items_of_same_order_df: sql.DataFrame,
                                        columns_of_customers: list[str],
                                        columns_of_orders: list[str]) -> sql.DataFrame:
        """Consolidate orders by customers."""
        self.logger.info("Consolidating the orders of each customer...")
        orders_of_same_customer_df = order_items_of_same_order_df. \
            select(
                *columns_of_customers,
                func.struct(*columns_of_orders, "order_items").alias("order_structs")
            ). \
            groupBy(
                *columns_of_customers
            ). \
            agg(func.collect_list("order_structs").alias("orders")). \
            orderBy("customer_id")
        self.logger.info("Consolidation finished!")
        return orders_of_same_customer_df

    def write_df(self, df: sql.DataFrame) -> None:
        """Write the dataframe to the specified folder."""
        self.logger.info("Now writing the dataframe to the output folder...")
        df.write.parquet(constants.OUTPUT_FOLDER_PATH, "overwrite")
        self.logger.info("Successfully wrote dataframe to PARQUET files!")

    def read_received_files(self) -> sql.DataFrame:
        """Read the files sent by the retailer into a dataframe."""
        self.logger.info("Now reading received files into a dataframe...")
        consolidated_df = self.spark.read.parquet(constants.OUTPUT_FOLDER_PATH)
        self.logger.info("Successfully created dataframe from PARQUET files!")
        return consolidated_df

    def flatten_df(self, consolidated_df: sql.DataFrame,
                   columns_of_customers: list[str],
                   columns_of_orders: list[str],
                   columns_of_order_items: list[str]) -> sql.DataFrame:
        """Flatten the consolidated dataframe."""
        self.logger.info("Now reading received files into a dataframe...")

        # when the field containing consolidated records are exploded, e.g. the field `orders`
        # we need to prefix the parent field when referencing the sub-fields, e.g. the sub-field `orders.order_id`
        # thus we need to alias the sub-fields so we can refer to them without the prefix, e.g. the sub-field `order_id`
        aliased_orders_columns = [func.col(f"orders.{name}").alias(name) for name in columns_of_orders]
        aliased_order_items_columns = [func.col(f"order_items.{name}").alias(name) for name in columns_of_order_items]

        flattened_df = consolidated_df. \
            select(*columns_of_customers, func.explode("orders").alias("orders")). \
            select(*columns_of_customers, *aliased_orders_columns, func.explode("orders.order_items").alias("order_items")). \
            select(*columns_of_customers, *columns_of_orders, *aliased_order_items_columns). \
            orderBy("customer_id", "order_id")

        self.logger.info("Flattening finished!")
        return flattened_df

    def split_df_into_multiple_dfs(self, flattened_df: sql.DataFrame,
                   columns_of_customers: list[str],
                   columns_of_orders: list[str],
                   columns_of_order_items: list[str]) -> tuple[sql.DataFrame, sql.DataFrame, sql.DataFrame]:
        """Split the flat dataframe into customers_df, orders_df and order_items_df."""
        self.logger.info("Now splitting the flattened dataframe into 3 dataframes...")
        customers_df = flattened_df.select(*columns_of_customers).distinct()
        orders_df = flattened_df.select(*columns_of_orders).distinct().dropna()
        order_items_df = flattened_df.select(*columns_of_order_items).distinct().dropna()
        self.logger.info("splitting finished!")
        return customers_df, orders_df, order_items_df
