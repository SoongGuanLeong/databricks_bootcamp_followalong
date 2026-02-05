# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

from pyspark.sql.types import DecimalType

df = (
    actor_rating_shift.drop("film_title")
    .withColumn("film_rating", F.col("film_rating").cast(DecimalType(10, 4)))
    .withColumn("max_date", F.max("release_date").over(W.partitionBy("actor_name")))
    .groupBy("actor_name")
    .agg(
        F.sum(
            F.when(
                F.col("release_date") == F.col("max_date"), F.col("film_rating")
            ).otherwise(F.lit(0).cast(DecimalType(10, 4)))
        ).alias("latest_rating"),
        F.when(
            F.count(F.col("actor_name")) == 1,
            F.sum(F.col("film_rating")).cast(DecimalType(10, 4)),
        )
        .otherwise(
            (
                F.sum(F.col("film_rating")).cast(DecimalType(10, 4))
                - F.sum(
                    F.when(
                        F.col("release_date") == F.col("max_date"), F.col("film_rating")
                    ).otherwise(F.lit(0).cast(DecimalType(10, 4)))
                )
            )
            / ((F.count("*") - 1).cast(DecimalType(10, 4)))
        )
        .alias("avg_rating"),
    )
    .withColumn(
        "rating_difference", F.round(F.col("latest_rating") - F.col("avg_rating"), 2)
    )
    .select(
        "actor_name",
        F.round("latest_rating", 2).alias("latest_rating"),
        F.round("avg_rating", 2).alias("avg_rating"),
        F.round("rating_difference", 2).alias("rating_difference"),
    )
)

df.toPandas()
