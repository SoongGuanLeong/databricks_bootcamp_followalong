# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
players_results = (
    players_results.orderBy(["player_id", "match_date"])
    .withColumn("lflag", F.when(F.col("match_result") == "L", 1).otherwise(0))
    .withColumn(
        "streakno",
        F.sum(F.col("lflag")).over(
            W.partitionBy("player_id")
            .orderBy(F.col("match_date"))
            .rowsBetween(W.unboundedPreceding, W.currentRow)
        ),
    )
    .withColumn(
        "maxstreakno", F.max(F.col("streakno")).over(W.partitionBy("player_id"))
    )
    .filter(F.col("streakno") > 0)
    .filter(F.col("streakno") < F.col("maxstreakno"))
    .groupBy(["player_id", "streakno"])
    .agg(F.sum(F.when(F.col("lflag") == 0, 1).otherwise(0)).alias("streak"))
    .groupBy("player_id")
    .agg(F.max(F.col("streak")).alias("streak"))
    .withColumn("overall_max", F.max(F.col("streak")).over(W.partitionBy()))
    .filter(F.col("streak") == F.col("overall_max"))
    .drop("overall_max")
)


# To validate your solution, convert your final pySpark df to a pandas df
players_results.toPandas()
