# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
sf_transactions = (
    sf_transactions.withColumn(
        "year_month", F.date_format(F.col("created_at"), "yyyy-MM")
    )
    .groupBy("year_month")
    .agg(F.sum(F.col("value")).alias("revenue"))
    .orderBy("year_month")
    .withColumn(
        "revenue_diff_pct",
        (
            F.col("revenue")
            - F.lag(F.col("revenue")).over(W.orderBy(F.col("year_month")))
        )
        / F.lag(F.col("revenue")).over(W.orderBy(F.col("year_month")))
        * 100,
    )
    .drop("revenue")
)

# To validate your solution, convert your final pySpark df to a pandas df
sf_transactions.toPandas()
