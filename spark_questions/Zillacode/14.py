from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(pe_firms, pe_funds, pe_investments):
	# Write code here
	df = (
        pe_firms.join(pe_funds, "firm_id", "outer")
            .join(pe_investments, "fund_id", "outer")
            .dropna(how="all")
    )

    return df