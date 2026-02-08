# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
employee = (
    employee.withColumn(
        "rnk",
        F.rank().over(W.partitionBy("department").orderBy(F.col("salary").desc())),
    )
    .filter(F.col("rnk") == 1)
    .select("department", "first_name", "salary")
)

# To validate your solution, convert your final pySpark df to a pandas df
employee.toPandas()
