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
    .distinct()
    .withColumn(
        "flag",
        F.when(
            F.datediff(F.col("created_at"), F.lag("created_at").over(w)) <= 7, 1
        ).otherwise(0),
    )
    .groupBy("user_id")
    .agg(F.max(F.col("flag")).alias("flag"))
    .filter(F.col("flag") == 1)
    .drop("flag")
)

# To validate your solution, convert your final pySpark df to a pandas df
amazon_transactions.toPandas()
