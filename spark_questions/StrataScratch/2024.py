# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
fact_events = (
    fact_events.withColumn("month", F.month(F.col("time_id")).alias("month"))
    .groupBy("client_id", "month")
    .agg(F.count_distinct(F.col("user_id")).alias("users_num"))
)

# To validate your solution, convert your final pySpark df to a pandas df
fact_events.toPandas()
