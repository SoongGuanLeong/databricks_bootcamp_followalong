# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
facebook_complaints = facebook_complaints.groupBy("type").agg(
    (
        F.sum(F.when(F.col("processed") == True, F.lit(1)).otherwise(F.lit(0)))
        / F.count("*")
    ).alias("processed_rate")
)

# To validate your solution, convert your final pySpark df to a pandas df
facebook_complaints.toPandas()
