from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName("run-pyspark-code").getOrCreate()


def etl(properties_df, landlords_df):
    # Write code here
    # dedup
    properties_df = properties_df.dropDuplicates()
    landlords_df = landlords_df.dropDuplicates()

    df = properties_df.join(landlords_df, "landlord_id").withColumn(
        "landlord_name", F.concat(F.col("first_name"), F.lit(" "), F.col("last_name"))
    )

    df_result = df.groupBy("landlord_id", "landlord_name").agg(
        F.sum("rent").alias("total_rental_income")
    )

    return df_result
