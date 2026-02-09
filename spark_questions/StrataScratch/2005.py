# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
fb_active_users = (
    fb_active_users
    .groupBy()
    .agg(
        (F.sum(F.when((F.col("country") == "USA") & (F.col("status") == "open"), F.lit(1)).otherwise(F.lit(0)))
        / F.count("*") * F.lit(100))
        .alias("us_active_share")
        )
    )

# To validate your solution, convert your final pySpark df to a pandas df
fb_active_users.toPandas()