from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(buildings):
	# Write code here
	df = buildings.withColumn("avg_height_per_floor", F.round(F.when(F.col("floors") == 0, 0)
                                                        .otherwise(F.col("height_m") / F.col("floors")), 2)
                             )

    return df.select("avg_height_per_floor", "city", "country", "id", "name")