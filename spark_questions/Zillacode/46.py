from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df_temperature, df_pressure):
	# Write code here
	df = (
        df_temperature.join(df_pressure, "ExperimentID")
            .withColumn("Result", F.col("Temperature") * F.col("Pressure"))
    )

    return df.select("ExperimentID", "Result")