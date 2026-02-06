# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
w = W.partitionBy("user_id").orderBy(F.col("date_visited"))

user_streaks = (
    user_streaks.distinct()
    .filter(F.col("date_visited") <= "2022-08-10")
    .withColumn(
        "flag",
        F.when(
            F.datediff(F.col("date_visited"), F.lag(F.col("date_visited")).over(w))
            == 1,
            0,
        ).otherwise(1),
    )
    .withColumn(
        "streak_id",
        F.sum("flag").over(W.rowsBetween(W.unboundedPreceding, W.currentRow)),
    )
    .groupBy("user_id", "streak_id")
    .agg(F.count("*").alias("dq"))
    .groupBy("user_id")
    .agg(F.max("dq").alias("dq"))
    .withColumn("drnk", F.dense_rank().over(W.orderBy(F.col("dq").desc())))
    .filter(F.col("drnk") <= 3)
    .drop("drnk")
)

# To validate your solution, convert your final pySpark df to a pandas df
user_streaks.toPandas()
