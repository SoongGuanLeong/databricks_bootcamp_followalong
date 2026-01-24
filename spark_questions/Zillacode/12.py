from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName("run-pyspark-code").getOrCreate()


def etl(input_df1, input_df2):
    # Write code here
    return input_df1.union(input_df2)
