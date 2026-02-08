# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
yelp_business = (
    yelp_business.filter(F.col("stars") == 5)
    .groupBy("state")
    .agg(F.count_distinct("business_id").alias("n_businesses"))
    .withColumn("drnk", F.dense_rank().over(W.orderBy(F.col("n_businesses").desc())))
    .filter(F.col("drnk") <= 5)
    .drop("drnk")
)

# To validate your solution, convert your final pySpark df to a pandas df
yelp_business.toPandas()
