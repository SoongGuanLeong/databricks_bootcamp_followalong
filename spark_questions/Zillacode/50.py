from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(artifacts):
	# Write code here
	df = (
        artifacts.filter(F.col("Quantity") > 100)
            .withColumn("Material", F.upper(F.col("Material")))
    )

    return df