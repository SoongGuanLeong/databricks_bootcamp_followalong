# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
user_flags = (
    user_flags.filter(~F.col("flag_id").isNull())
    .withColumn(
        "username", F.concat_ws(" ", F.col("user_firstname"), F.col("user_lastname"))
    )
    .groupBy("video_id")
    .agg(F.count_distinct(F.col("username")).alias("num_unique_users"))
)

# To validate your solution, convert your final pySpark df to a pandas df
user_flags.toPandas()
