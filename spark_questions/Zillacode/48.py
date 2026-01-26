from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(customers, orders, products):
	# Write code here
	customers = (
        customers.withColumn("first_name", F.split(F.col("full_name"), " ", 2)[0])
            .withColumn("last_name", F.split(F.col("full_name"), " ", 2)[1])
            .select("customer_id", "first_name", "last_name", "location")
    )

    products = (
        products.withColumn("product_type", F.split(F.col("product_info"), ",", 2)[0])
            .withColumn("product_color", F.split(F.col("product_info"), ",", 2)[1])
            .select("product_id", "product_type", "product_color")
    )

    df = orders.join(customers, "customer_id").join(products, "product_id")

    return df