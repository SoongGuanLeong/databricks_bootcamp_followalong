# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
billboard_top_100_year_end = (
    billboard_top_100_year_end.filter(F.col("year") == 2010)
    .filter(F.col("year_rank").between(1, 10))
    .select("year_rank", "group_name", "song_name")
    .distinct()
    .orderBy("year_rank")
)

# To validate your solution, convert your final pySpark df to a pandas df
billboard_top_100_year_end.toPandas()
