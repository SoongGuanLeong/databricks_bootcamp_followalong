# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
yelp_reviews = (
    yelp_reviews.withColumn("rank", F.rank().over(W.orderBy(F.col("cool").desc())))
    .filter(F.col("rank") == 1)
    .select("business_name", "review_text")
)

# To validate your solution, convert your final pySpark df to a pandas df
yelp_reviews.toPandas()
