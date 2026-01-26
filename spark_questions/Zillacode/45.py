from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df1, df2):
	# Write code here
	df = (
        df1.union(df2)
    )

    ranges = (
        df.groupBy("Country")
            .agg(
                F.min(F.col("Year")).alias("min_year"),
                F.max(F.col("Year")).alias("max_year")
            )
    )

    year_seq = (
        ranges.withColumn("Year", F.explode(F.sequence(F.col("min_year"), F.col("max_year"))))
            .select("Country", "Year")
    )

    df = (
        year_seq.join(df, ["Country", "Year"], "left")
            .withColumn("GDP_growth_rate", 
                        F.round(
                        (F.col("GDP") 
                        - F.lag(F.col("GDP")).over(W.partitionBy("Country").orderBy(F.col("Year"))))
                        / F.lag(F.col("GDP")).over(W.partitionBy("Country").orderBy(F.col("Year")))
                        * 100, 2)
                       )
    )

    return df.select("Country", "GDP_growth_rate", "Year")
