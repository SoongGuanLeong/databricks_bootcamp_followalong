# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
airbnb_search_details = (
    airbnb_search_details.select("city", "amenities")
    .withColumn("test", F.size(F.split(F.col("amenities"), ",", -1)))
    .groupBy("city")
    .agg(F.sum(F.col("test")).alias("total"))
    .withColumn("rnk", F.rank().over(W.orderBy(F.col("total").desc())))
    .filter(F.col("rnk") == 1)
    .select("city")
)

# To validate your solution, convert your final pySpark df to a pandas df
airbnb_search_details.toPandas()
