# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
user_flags = user_flags.withColumn(
    "username", F.concat(F.col("user_firstname"), F.lit(" "), F.col("user_lastname"))
)

flag_review = flag_review.filter(F.col("reviewed_outcome") == "APPROVED")

user_flags = (
    user_flags.join(flag_review, "flag_id")
    .select("username", "video_id")
    .distinct()
    .groupBy("username")
    .agg(F.count("*").alias("cnt"))
    .withColumn("rnk", F.rank().over(W.orderBy(F.col("cnt").desc())))
    .filter(F.col("rnk") == 1)
    .select("username")
)

# To validate your solution, convert your final pySpark df to a pandas df
user_flags.toPandas()
