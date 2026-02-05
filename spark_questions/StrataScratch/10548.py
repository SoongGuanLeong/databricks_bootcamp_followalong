# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
top_actors_rating = (
    top_actors_rating.select("actor_name", "genre", "movie_rating")
    .groupBy("actor_name", "genre")
    .agg(F.count("*").alias("cnt"), F.avg(F.col("movie_rating")).alias("avg_rating"))
    .withColumn(
        "rnk",
        F.rank().over(
            W.partitionBy("actor_name").orderBy(
                [F.col("cnt").desc(), F.col("avg_rating").desc()]
            )
        ),
    )
    .filter(F.col("rnk") == 1)
    .drop("rnk", "cnt")
    .withColumn(
        "actor_rank", F.dense_rank().over(W.orderBy(F.col("avg_rating").desc()))
    )
    .filter(F.col("actor_rank") <= 3)
)

# To validate your solution, convert your final pySpark df to a pandas df
top_actors_rating.toPandas()
