# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
accepted = (
    fb_friend_requests.filter(F.col("action") == "accepted")
    .drop("date")
    .withColumnRenamed("action", "accepted")
)

fb_friend_requests = (
    fb_friend_requests.filter(F.col("action") == "sent")
    .join(accepted, on=["user_id_sender", "user_id_receiver"], how="left")
    .groupBy("date")
    .agg(
        (F.count(F.col("accepted") == "accepted") / F.count("*")).alias(
            "percentage_acceptance"
        )
    )
)

# To validate your solution, convert your final pySpark df to a pandas df
fb_friend_requests.toPandas()
