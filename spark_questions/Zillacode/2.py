from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(customers, orders, products):
	# Write code here
    df = (
        customers.join(orders, "customer_id")
            .join(products, "product_id")
            .withColumn("customer_name", F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")))
    )

    df_result = df.select("category", "customer_name", "email", "order_date", "order_id", "product_name")
	return df_result