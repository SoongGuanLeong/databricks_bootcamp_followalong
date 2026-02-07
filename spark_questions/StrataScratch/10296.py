# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
fb_account_status = (
    fb_account_status.filter(F.col("status_date") == "2020-01-10")
    .groupBy()
    .agg(
        F.round(
            F.sum(F.when(F.col("status") == "closed", 1).otherwise(0)) / F.count("*"), 2
        ).alias("closed_ratio")
    )
)

# To validate your solution, convert your final pySpark df to a pandas df
fb_account_status.toPandas()
