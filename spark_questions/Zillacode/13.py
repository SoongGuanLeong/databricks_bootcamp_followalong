from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(mountain_info, mountain_climbers):
	# Write code here
    w = W.partitionBy("mountain_name").orderBy(F.col("climb_date").desc())
    
	df = (
        mountain_climbers.withColumn("rank", F.row_number().over(w))
            .filter(F.col("rank") == 1)
            .drop("rank")
    )

    return df
    