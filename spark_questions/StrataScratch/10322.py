# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
w = W.partitionBy("user_id").orderBy(F.col("created_at"))

amazon_transactions = (
    amazon_transactions.select("user_id", "created_at")
    .withColumn("drnk", F.dense_rank().over(w))
    .filter(F.col("drnk").between(1, 2))
    .withColumn(
        "flag",
        F.when(
            F.datediff(F.col("created_at"), F.lag(F.col("created_at")).over(w)).between(
                1, 7
            ),
            1,
        ).otherwise(0),
    )
    .groupBy("user_id")
    .agg(F.max(F.col("flag")).alias("is_returning_user"))
    .filter(F.col("is_returning_user") == 1)
    .select("user_id")
)

# To validate your solution, convert your final pySpark df to a pandas df
amazon_transactions.toPandas()
