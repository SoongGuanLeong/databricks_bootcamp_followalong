from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(venture_capitalist_df, funded_startups_df):
	# Write code here
	funded_startups_df = (
        funded_startups_df.groupBy("vc_id")
            .agg(
                F.avg(F.col("funding")).alias("avg_funding")
            )
    )

    df = (
        venture_capitalist_df.join(funded_startups_df, "vc_id")
            .filter(F.col("avg_funding") > F.col("funding_limit"))
    )

    return df