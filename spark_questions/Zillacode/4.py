from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName("run-pyspark-code").getOrCreate()


def etl(input_df):
    # Write code here
    df = (
        input_df.withColumnRenamed("email", "email_domain")
        .withColumnRenamed("phone", "anon_phone")
        .withColumn("email_domain", F.split(F.col("email_domain"), "@")[1])
        .withColumn(
            "anon_phone",
            F.concat(F.lit("******"), F.substring(F.col("anon_phone"), 7, 4)),
        )
    )

    return df
