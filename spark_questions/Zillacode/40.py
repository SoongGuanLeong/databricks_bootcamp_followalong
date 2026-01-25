from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(background_checks):
	# Write code here
	df = (
        background_checks.withColumn("crime_count", F.size(F.split(F.col("criminal_record"), ",")))
            .withColumn("degrees_count", F.size(F.split(F.col("education_history"), ",")))
            .withColumn("jobs_count", F.size(F.split(F.col("employment_history"), ",")))
            .withColumn("places_lived_count", F.size(F.split(F.col("address"), ",")))
    )

    return df.select("check_id", "crime_count", "degrees_count", "dob", "full_name", "jobs_count", "places_lived_count")