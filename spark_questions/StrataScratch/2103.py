# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
user_flags = (
    user_flags.filter(~F.col("flag_id").isNull())
    .withColumn("cnt", F.count(F.col("flag_id")).over(W.partitionBy("video_id")))
    .withColumn("max_cnt", F.max(F.col("cnt")).over(W.partitionBy()))
    .filter(F.col("cnt") == F.col("max_cnt"))
    .join(flag_review, "flag_id", how="left")
    .groupBy("video_id")
    .agg(
        F.sum(F.when(F.col("reviewed_by_yt") == True, 1).otherwise(0)).alias(
            "reviewed_by_yt"
        )
    )
)

# To validate your solution, convert your final pySpark df to a pandas df
user_flags.toPandas()
