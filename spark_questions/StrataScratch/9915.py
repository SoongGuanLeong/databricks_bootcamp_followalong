# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
orders = (
    orders.filter(F.col("order_date").between("2019-02-01", "2019-05-01"))
    .groupBy(["cust_id", "order_date"])
    .agg(F.sum(F.col("total_order_cost")).alias("total_order_cost"))
    .withColumn(
        "rank",
        F.rank().over(
            W.partitionBy("order_date").orderBy(F.col("total_order_cost").desc())
        ),
    )
    .filter(F.col("rank") == 1)
    .drop("rank")
    .withColumnRenamed("total_order_cost", "max_cost")
)

customers = customers.join(orders, orders.cust_id == customers.id).select(
    "first_name", "order_date", "max_cost"
)

# To validate your solution, convert your final pySpark df to a pandas df
customers.toPandas()
