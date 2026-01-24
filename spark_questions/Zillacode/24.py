from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df_accounts, df_activities, df_exit_surveys):
	# Write code here
	df_activities = df_activities.dropDuplicates()

    df = (
        df_accounts.join(df_activities, "user_id", "outer")
            .join(df_exit_surveys, "user_id", "outer")
            .orderBy(["user_id", "activity_date"], ascending=[True, False])
    )

    return df