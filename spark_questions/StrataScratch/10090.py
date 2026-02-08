# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
customers = customers.withColumnRenamed("id", "cust_id")

orders = orders.join(customers, "cust_id", how="left").agg(
    (
        F.sum(F.when(F.col("address").isNull(), F.lit(0)).otherwise(F.lit(1)))
        / F.count("*")
        * 100
    ).alias("percent_shipable")
)

# To validate your solution, convert your final pySpark df to a pandas df
orders.toPandas()
