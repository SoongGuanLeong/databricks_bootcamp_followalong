# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
playbook_events = (
    playbook_events.withColumn(
        "flag",
        F.when(
            F.col("device").isin("macbook pro", "iphone 5s", "ipad air"), F.lit(1)
        ).otherwise(F.lit(0)),
    )
    .withColumn("overallFlag", F.max(F.col("flag")).over(W.partitionBy("user_id")))
    .join(playbook_users, "user_id")
    .select("user_id", "overallFlag", "language")
    .distinct()
    .groupBy("language")
    .agg(
        F.sum(F.col("overallFlag")).alias("n_apple_users"),
        F.count("*").alias("n_total_users"),
    )
    .orderBy(F.col("n_total_users").desc())
)

# To validate your solution, convert your final pySpark df to a pandas df
playbook_events.toPandas()
