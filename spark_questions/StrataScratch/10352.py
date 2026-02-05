# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
facebook_web_log = facebook_web_log.filter(
    (F.col("action") == "page_load") | (F.col("action") == "page_exit")
).withColumn("action_date", F.date_format(F.col("timestamp"), "yyyy-MM-dd"))

load_log = (
    facebook_web_log.filter(F.col("action") == "page_load")
    .groupBy("user_id", "action_date")
    .pivot("action")
    .agg(F.max("timestamp"))
)

exit_log = (
    facebook_web_log.filter(F.col("action") == "page_exit")
    .groupBy("user_id", "action_date")
    .pivot("action")
    .agg(F.min("timestamp"))
)

facebook_web_log = (
    load_log.join(exit_log, on=["user_id", "action_date"])
    .withColumn(
        "session_duration",
        F.unix_timestamp("page_exit") - F.unix_timestamp("page_load"),
    )
    .groupBy("user_id")
    .agg(F.avg("session_duration").alias("avg_session_duration"))
)

# To validate your solution, convert your final pySpark df to a pandas df
facebook_web_log.toPandas()
