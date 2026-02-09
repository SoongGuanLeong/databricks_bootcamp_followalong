# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
cookbook_titles = (
    cookbook_titles.withColumn("isOdd", F.col("page_number") % 2)
    .withColumn("left_page_number", F.col("page_number") - F.col("isOdd"))
    .groupBy("left_page_number")
    .pivot("isOdd")
    .agg(F.max(F.col("title")))
    .withColumnRenamed("0", "left_title")
    .withColumnRenamed("0", "right_title")
)

# To validate your solution, convert your final pySpark df to a pandas df
cookbook_titles.toPandas()
