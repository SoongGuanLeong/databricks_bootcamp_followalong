# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
orders = (
    orders.filter(F.col("order_date").between("2019-03-01", "2019-03-31"))
    .groupBy("cust_id")
    .agg(F.sum(F.col("total_order_cost")).alias("revenue"))
    .orderBy(F.col("revenue").desc())
)

# To validate your solution, convert your final pySpark df to a pandas df
orders.toPandas()
