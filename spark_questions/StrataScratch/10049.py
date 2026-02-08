# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
yelp_business = (
    yelp_business
    .withColumn("category", F.explode(F.split(F.col("categories"), ";", -1)))
    .groupBy("category")
    .agg(
        F.sum(F.col("review_count")).alias("review_cnt")
        )
    .orderBy(F.col("review_cnt").desc())
    )

# To validate your solution, convert your final pySpark df to a pandas df
yelp_business.toPandas()