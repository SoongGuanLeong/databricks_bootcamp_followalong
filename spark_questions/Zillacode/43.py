from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(observations, species):
	# Write code here
	df = (
        observations.join(species, "species_id")
            .withColumn("rank", F.rank().over(W.orderBy(F.col("count").desc())))
            .filter(F.col("rank") <= 3)
            .drop("rank")
    )

    return df