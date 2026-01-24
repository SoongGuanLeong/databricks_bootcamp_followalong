from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df_models, df_usage):
	# Write code here
	df_usage = (
        df_usage.drop("Date")
            .groupBy("Model_ID")
            .agg(
                F.sum(F.col("Uses")).alias("Total_Uses")
            )
    )

    df_models = df_models.withColumn("Average_Accuracy", F.avg(F.col("Accuracy")).over(W.partitionBy("Model_Type")))

    df = (
        df_usage.join(df_models, "Model_ID")
            
    )

    return df