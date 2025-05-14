"""This module defines the """


from util import spark_utils as su


def main() -> None:
    """Application entrypoint."""
    with su.SparkContextManager("retailer_app") as scm:
        scm.read_files()
        scm.customers_df.printSchema()
        scm.orders_df.printSchema()
        scm.order_items_df.printSchema()

        print()


if __name__ == "__main__":
    main()
