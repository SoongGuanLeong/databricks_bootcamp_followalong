# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
voting_results = (
    voting_results.filter(~F.col("candidate").isNull())
    .withColumn("pts", F.lit(1) / F.count("*").over(W.partitionBy("voter")))
    .groupBy("candidate")
    .agg(F.sum(F.col("pts")).alias("total_pts"))
    .withColumn("rnk", F.rank().over(W.orderBy(F.col("total_pts").desc())))
    .filter(F.col("rnk") == 1)
    .select("candidate")
)

# To validate your solution, convert your final pySpark df to a pandas df
voting_results.toPandas()
