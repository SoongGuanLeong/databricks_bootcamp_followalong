# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
cust_tracking = (
    cust_tracking.withColumn("timestamp_ts", F.unix_timestamp("timestamp"))
    .groupBy("cust_id")
    .pivot("state")
    .agg(F.sum(F.col("timestamp_ts")))
    .withColumn("sum(time_diff)", (F.col("0") - F.col("1")) / 3600)
    .drop("0", "1")
)

# To validate your solution, convert your final pySpark df to a pandas df
cust_tracking.toPandas()
