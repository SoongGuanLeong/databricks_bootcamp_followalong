# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
fintech_app_users = (
    fintech_app_users.filter(F.col("phone_number").isNull())
    .select("user_id", "user_name")
    )

# To validate your solution, convert your final pySpark df to a pandas df
fintech_app_users.toPandas()