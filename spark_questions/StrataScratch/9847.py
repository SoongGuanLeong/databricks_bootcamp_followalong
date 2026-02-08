# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
worker = (
    worker.filter(F.col("joining_date") >= "2014-04-01")
    .groupBy("department")
    .agg(F.count("*").alias("num_workers"))
    .orderBy(F.col("num_workers").desc())
)

# To validate your solution, convert your final pySpark df to a pandas df
worker.toPandas()
