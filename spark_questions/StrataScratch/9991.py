# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
spotify_worldwide_daily_song_ranking = (
    spotify_worldwide_daily_song_ranking.filter(F.col("position") == 1)
    .groupBy("trackname")
    .agg(F.count("*").alias("times_top1"))
    .orderBy(F.col("times_top1").desc())
)

# To validate your solution, convert your final pySpark df to a pandas df
spotify_worldwide_daily_song_ranking.toPandas()
