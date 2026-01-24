from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(budget_df, spending_df):
	# Write code here
	budget_df = (
        budget_df.groupBy("Department")
            .agg(
                F.variance(F.col("Budget")).cast("int").alias("Budget_Variance")
            )
    )

    spending_df = (
        spending_df.groupBy("Department")
            .agg(
                F.variance(F.col("Spending")).cast("int").alias("Spending_Variance")
            )
    )

    df = budget_df.join(spending_df, "Department")

    return df