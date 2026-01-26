from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df_star, df_planet):
	# Write code here
	df_star = (
        df_star.withColumnRenamed("id", "star_id")
            .withColumnRenamed("name", "star_name")
            .withColumnRenamed("color", "star_color")
            .withColumnRenamed("type", "star_type")
            .withColumnRenamed("distance", "distance_star_earth")
    )

    df_planet = (
        df_planet.withColumnRenamed("name", "planet_name")
            .withColumnRenamed("type", "planet_type")
            .withColumnRenamed("distance", "distance_planet_star")
    )

    df = (
        df_star.join(df_planet, "star_id")
            .drop("id", "star_id")
    )

    return df
