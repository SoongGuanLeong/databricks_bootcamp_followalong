# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
worker = (
    worker.filter(F.col("department") == "Admin")
    .withColumn("mo", F.month(F.col("joining_date")))
    .filter(F.col("mo") >= 4)
    .groupBy()
    .agg(F.count("*").alias("n_admins"))
)

# To validate your solution, convert your final pySpark df to a pandas df
worker.toPandas()
