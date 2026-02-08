# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
yelp_business = (
    yelp_business.groupBy("name")
    .agg(F.sum(F.col("review_count")).alias("review_count"))
    .withColumn("rnk", F.rank().over(W.orderBy(F.col("review_count").desc())))
    .filter(F.col("rnk") <= 5)
    .drop("rnk")
)

# To validate your solution, convert your final pySpark df to a pandas df
yelp_business.toPandas()
