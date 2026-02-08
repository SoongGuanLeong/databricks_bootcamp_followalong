# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
employee = (
    employee.withColumn("drnk", F.dense_rank().over(W.orderBy(F.col("salary").desc())))
    .filter(F.col("drnk") == 2)
    .select("salary")
    .distinct()
)

# To validate your solution, convert your final pySpark df to a pandas df
employee.toPandas()
