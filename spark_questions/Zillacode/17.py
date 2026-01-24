from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(input_df):
	# Write code here
	df = (
        input_df.withColumn("age", F.coalesce(F.regexp_extract(F.col("description"), r"[0-9]+", 0), F.lit("")
                                             )
                           )
    )

    return df