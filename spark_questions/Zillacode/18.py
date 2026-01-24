from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName("run-pyspark-code").getOrCreate()


def etl(products_df, manufacturing_processes_df):
    # Write code here
    products_df = products_df.dropDuplicates()
    manufacturing_processes_df = manufacturing_processes_df.dropDuplicates()

    df = products_df.join(manufacturing_processes_df, "ProductID")

    return df
