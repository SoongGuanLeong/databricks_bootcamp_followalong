from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df):
	# Write code here
	df = df.withColumn("Discount", F.coalesce(
                       F.regexp_extract(F.col("Description"), r"\[([0-9]+)% off\]", 1).cast("double") / 100.0, 
                        F.lit(0))
                      )

    return df