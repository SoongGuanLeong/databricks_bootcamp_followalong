from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(rides, visitors):
	# Write code here
    visitors = (
        visitors.groupBy("ride_id")
            .agg(
                F.avg(F.col("rating")).alias("average_rating")
            )
    )

    stats = visitors.agg(
        F.avg("average_rating").alias("global_avg"),
        F.stddev("average_rating").alias("global_std")
    ).collect()[0]

    visitors = (
        visitors.withColumn("is_anomalous", F.when(F.abs((F.col("average_rating")- F.lit(stats["global_avg"]))/ F.lit(stats["global_std"])) > 2, 1
                                                  ).otherwise(0))
    )
    
	df = (
        rides.join(visitors, "ride_id")
    )

    return df