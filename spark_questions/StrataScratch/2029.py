# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
fact_events = (
    fact_events.withColumn(
        "flag",
        F.when(
            F.col("event_type").isin(
                "video call received",
                "video call sent",
                "voice call received",
                "voice call sent",
            ),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )
    .select("user_id", "client_id", "flag")
    .withColumn(
        "user_flag",
        F.sum(F.col("flag")).over(W.partitionBy("user_id"))
        / F.count("*").over(W.partitionBy("user_id")),
    )
    .drop("flag")
    .distinct()
    .groupBy("client_id")
    .agg(
        F.sum(F.when(F.col("user_flag") >= 0.5, F.lit(1)).otherwise(F.lit(0))).alias(
            "client_flag"
        )
    )
    .withColumn("rnk", F.rank().over(W.orderBy(F.col("client_flag").desc())))
    .filter(F.col("rnk") == 1)
    .select("client_id")
)

# To validate your solution, convert your final pySpark df to a pandas df
fact_events.toPandas()
