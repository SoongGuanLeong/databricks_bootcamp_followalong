# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
airbnb_host_searches = (
    airbnb_host_searches.filter(F.col("number_of_reviews") >= 0)
    .withColumn(
        "host_popularity",
        F.when(F.col("number_of_reviews") == 0, F.lit("New"))
        .when(F.col("number_of_reviews").between(1, 5), F.lit("Rising"))
        .when(F.col("number_of_reviews").between(6, 15), F.lit("Trending Up"))
        .when(F.col("number_of_reviews").between(16, 40), F.lit("Popular"))
        .otherwise(F.lit("Hot")),
    )
    .groupBy("host_popularity")
    .agg(
        F.min(F.col("price")).alias("min_price"),
        F.avg(F.col("price")).alias("avg_price"),
        F.max(F.col("price")).alias("max_price"),
    )
    .orderBy("min_price")
)

# To validate your solution, convert your final pySpark df to a pandas df
airbnb_host_searches.toPandas()
