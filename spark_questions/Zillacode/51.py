from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df_math_expr):
	# Write code here
	df = (
        df_math_expr.filter(F.col("expression").rlike(r"^[0-9]+([+\-*/][0-9]+)+$"))
    )

    return df