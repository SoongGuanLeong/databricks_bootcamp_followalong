# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
sf_events = (
    sf_events
    .withColumn("year_month", F.date_format(F.col("record_date"), "yyyy-MM"))
    .drop("record_date")
    .distinct()
    .withColumn("user_acc_max", F.max("year_month").over(W.partitionBy("account_id", "user_id")))
    .withColumn("flag", F.when(F.col("user_acc_max") > F.col("year_month"), F.lit(1)).otherwise(F.lit(0)))
    .groupBy("account_id")
    .agg((
        (F.sum(F.when(F.col("year_month") == "2021-01", F.col("flag")).otherwise(F.lit(0)))
        / F.sum(F.when(F.col("year_month") == "2021-01", F.lit(1)).otherwise(F.lit(0))))
        /
        (F.sum(F.when(F.col("year_month") == "2020-12", F.col("flag")).otherwise(F.lit(0)))
        / F.sum(F.when(F.col("year_month") == "2020-12", F.lit(1)).otherwise(F.lit(0))))
        ).alias("retention"))
    )

# To validate your solution, convert your final pySpark df to a pandas df
sf_events.toPandas()