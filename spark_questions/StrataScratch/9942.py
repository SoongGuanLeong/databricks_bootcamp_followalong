# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
olympics_athletes_events = (
    olympics_athletes_events.select("games", "id")
    .distinct()
    .groupBy("games")
    .agg(F.count("*").alias("athletes_count"))
    .withColumn("rnk", F.rank().over(W.orderBy(F.col("athletes_count").desc())))
    .filter(F.col("rnk") == 1)
    .drop("rnk")
)

# To validate your solution, convert your final pySpark df to a pandas df
olympics_athletes_events.toPandas()
