# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
sat_scores = (
    sat_scores.withColumn(
        "median_sat_writing", F.median(F.col("sat_writing")).over(W.partitionBy())
    )
    .filter(F.col("sat_writing") == F.col("median_sat_writing"))
    .select("student_id")
)

# To validate your solution, convert your final pySpark df to a pandas df
sat_scores.toPandas()
