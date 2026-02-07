# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
w = W.partitionBy("state").orderBy(F.col("fraud_score"))

fraud_score = (
    fraud_score.withColumn("pct_rnk", F.percent_rank().over(w))
    .filter(F.col("pct_rnk") >= F.lit(0.95))
    .drop("pct_rnk")
)

# To validate your solution, convert your final pySpark df to a pandas df
fraud_score.toPandas()
