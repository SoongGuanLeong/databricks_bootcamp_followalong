from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(flights, airports, planes):
	# Write code here    
	airports = (
        airports.withColumn("airport_name_length", F.length(F.col("airport_name")))
            .drop("airport_name")
    )

    a1 = airports.alias("a1")
    a2 = airports.alias("a2")
    

    planes = (
        planes.withColumn("plane_model_length", F.length(F.col("plane_model")))
            .drop("plane_model")
    )

    df = (
        flights.join(a1, flights.origin_airport == a1.airport_id)
            .withColumnRenamed("airport_name_length", "origin_airport_name_length")
            .join(a2, flights.destination_airport == a2.airport_id)
            .withColumnRenamed("airport_name_length", "destination_airport_name_length")
            .join(planes, flights.flight_id == planes.plane_id)
            .drop("origin_airport", "destination_airport", "plane_id", "airport_id")
    )

    return df