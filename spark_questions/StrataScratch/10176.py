# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
dc_bikeshare_q1_2012 = (
    dc_bikeshare_q1_2012.select("bike_number", "end_time")
    .groupBy("bike_number")
    .agg(F.max(F.col("end_time")).alias("last_used"))
    .orderBy(F.col("last_used").desc())
)

# To validate your solution, convert your final pySpark df to a pandas df
dc_bikeshare_q1_2012.toPandas()
