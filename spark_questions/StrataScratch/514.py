# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
w = W.partitionBy("user_id")

marketing_campaign = (
    marketing_campaign
    .withColumn("first_date", F.min("created_at").over(w))
    .withColumn("first_day_product", F.collect_set(F.when(F.col("created_at") == F.col("first_date"), F.col("product_id"))).over(w))
    .filter(F.col("created_at") != F.col("first_date"))
    .filter(~F.array_contains(F.col("first_day_product"), F.col("product_id")))
    .select("user_id").distinct()
    .count()
    )

# To validate your solution, convert your final pySpark df to a pandas df
marketing_campaign