from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(user_behavior_df, subscription_df):
	# Write code here
	df = (
        user_behavior_df.join(subscription_df, "userId")
            .withColumn("isactive", 
                        F.when((F.col("date") >= F.col("subscriptionStart")) & (F.col("subscriptionEnd") == F.lit("ongoing")), True)
                        .when((F.col("date") >= F.col("subscriptionStart")) & (F.col("date") <= F.col("subscriptionEnd")), True)
                        .otherwise(False))
            .groupBy("userId")
            .agg(
                F.sum(F.when(F.col("isactive") == True, F.col("watchDuration"))).alias("totalWatchTime")
            )
    )

    return df