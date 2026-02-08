# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
facebook_employees = (
    facebook_employees.withColumnRenamed("id", "employee_id")
    .join(facebook_hack_survey, "employee_id")
    .groupBy("location")
    .agg(F.avg(F.col("popularity")).alias("popularity"))
)

# To validate your solution, convert your final pySpark df to a pandas df
facebook_employees.toPandas()
