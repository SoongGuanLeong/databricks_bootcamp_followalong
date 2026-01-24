from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(companies, investments):
	# Write code here
	df = (
        companies.join(investments, "company_id")
            .groupBy("industry")
            .agg(
                F.sum(F.col("amount")).alias("total_investment")
            )
            .orderBy("total_investment")    
    )

    df_result = df.select("industry", "total_investment")

    return df_result