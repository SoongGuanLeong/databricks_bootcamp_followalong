# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
w = W.partitionBy("department")

employee = employee.withColumn("avg_salary", F.avg(F.col("salary")).over(w)).select(
    "department", "first_name", "salary", "avg_salary"
)

# To validate your solution, convert your final pySpark df to a pandas df
employee.toPandas()
