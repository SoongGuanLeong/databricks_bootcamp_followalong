from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(input_df):
	# Write code here
	input_df = input_df.withColumn("flag", (F.col("user1_id") == F.col("user2_id")).cast("int"))

    df = (
        input_df.groupBy("user1_id")
            .agg(
                F.sum(F.col("flag")).alias("self_interaction_count")
            )
            .filter(F.col("self_interaction_count") >= 1)
    )

    return df
