from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName("run-pyspark-code").getOrCreate()


def etl(products, sales, inventory):
    # Write code here
    from pyspark.sql.types import IntegerType

    inventory = inventory.groupBy("product_id").agg(
        F.sum(F.col("stock")).cast(IntegerType()).alias("total_stock")
    )

    df = (
        sales.join(products, "product_id", "outer")
        .join(inventory, "product_id", "outer")
        .withColumn("quantity", F.coalesce(F.col("quantity"), F.lit(0)))
        .withColumn("revenue", F.coalesce(F.col("revenue"), F.lit(0)))
        .withColumn("total_stock", F.coalesce(F.col("total_stock"), F.lit(0)))
    )

    df_result = df.groupBy(["category", "name", "product_id", "total_stock"]).agg(
        F.sum(F.col("quantity")).cast(IntegerType()).alias("total_quantity"),
        F.sum(F.col("revenue")).cast(IntegerType()).alias("total_revenue"),
    )

    return df_result
