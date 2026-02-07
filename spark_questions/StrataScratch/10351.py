# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
google_gmail_emails = (
    google_gmail_emails.groupBy("from_user")
    .agg(F.count("*").alias("total_emails"))
    .withColumn(
        "activity_rank",
        F.row_number().over(
            W.orderBy(F.col("total_emails").desc(), F.col("from_user"))
        ),
    )
    .withColumnRenamed("from_user", "user_id")
)

# To validate your solution, convert your final pySpark df to a pandas df
google_gmail_emails.toPandas()
