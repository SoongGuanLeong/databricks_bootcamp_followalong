from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df_transactions, df_clients):
	# Write code here
	df_clients = (
        df_clients.filter(F.col("ClientID") >= 1)
            .dropDuplicates(["ClientID"])
    )

    df_transactions = (
        df_transactions.filter(F.col("ClientID") >= 1)
            .filter(F.col("TransactionID") >= 1)
            .dropDuplicates(["TransactionID"])
            .join(df_clients.select("ClientID"), "ClientID", "left_semi")
    )

    df = df_transactions.join(df_clients, "ClientID")

    return df