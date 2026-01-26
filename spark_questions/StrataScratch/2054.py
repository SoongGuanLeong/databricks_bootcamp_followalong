# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
w = W.partitionBy("user_id").orderBy(F.col("record_date"))

sf_events = (
    sf_events.withColumn(
        "isnewstreak",
        F.when(F.datediff(F.col("record_date"), F.lag("record_date").over(w)) != 1, 1)
        .when(F.lag("record_date").over(w).isNull(), 1)
        .otherwise(0),
    )
    .drop("account_id")
    .withColumn(
        "streakno",
        F.sum(F.col("isnewstreak")).over(
            W.rowsBetween(W.unboundedPreceding, W.currentRow)
        ),
    )
    .groupBy(["user_id", "streakno"])
    .agg(F.count(F.col("streakno")).alias("contdays"))
    .filter(F.col("contdays") >= 3)
    .select("user_id")
)

# To validate your solution, convert your final pySpark df to a pandas df
sf_events.toPandas()
