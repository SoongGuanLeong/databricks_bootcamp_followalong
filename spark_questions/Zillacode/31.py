from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df_orders, df_products):
	# Write code here
	df_orders = (
        df_orders.withColumn("order_date", F.to_date(F.col("order_date"), "MM/dd/yyyy"))
            .filter(F.col("order_date").isNotNull())
            .withColumn("is_weekend", ((F.dayofweek("order_date") == 1) | (F.dayofweek("order_date") == 7)).cast("int"))
            .withColumn("order_date", F.date_format(F.col("order_date"), "MM/dd/yyyy"))
    )

    df = (
        df_orders.join(df_products, "product_id")
            .select("category", "is_weekend", "order_date", "product_name", "user_id")
    )

    return df