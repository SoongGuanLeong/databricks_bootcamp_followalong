from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName("run-pyspark-code").getOrCreate()


def etl(products_df, orders_df):
    # Write code here
    df = orders_df.join(products_df, "product_id")

    df_result = df.groupBy("category").agg(
        F.avg(F.col("price")).alias("avg_price"),
        F.count("category").alias("total_orders_count"),
    )

    return df_result
