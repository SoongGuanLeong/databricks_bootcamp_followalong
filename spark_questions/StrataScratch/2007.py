# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
fb_comments_count = (
    fb_comments_count.filter(F.col("created_at").between("2019-12-01", "2020-01-31"))
    .withColumn("month", F.month("created_at"))
    .join(fb_active_users, "user_id")
    .groupBy(["country", "month"])
    .agg(F.sum(F.col("number_of_comments")).alias("total_comments"))
    .withColumn(
        "drank",
        F.dense_rank().over(
            W.partitionBy("month").orderBy(F.col("total_comments").desc())
        ),
    )
    .groupBy("country")
    .pivot("month")
    .agg(F.sum("drank"))
    .filter(F.col("12") > F.col("1"))
    .select("country")
)

# To validate your solution, convert your final pySpark df to a pandas df
fb_comments_count.toPandas()
