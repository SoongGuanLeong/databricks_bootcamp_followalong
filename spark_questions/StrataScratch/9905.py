# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
salesforce_employees = (
    salesforce_employees.filter(F.col("manager_id") == 13)
    .withColumn("rnk", F.rank().over(W.orderBy(F.col("target").desc())))
    .filter(F.col("rnk") == 1)
    .select("first_name", "target")
)

# To validate your solution, convert your final pySpark df to a pandas df
salesforce_employees.toPandas()
