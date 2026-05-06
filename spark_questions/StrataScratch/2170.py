# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
techcorp_workforce = (
    techcorp_workforce.filter(F.year("joining_date") > F.lit(2020))
    .groupBy("department")
    .agg(
        F.count(F.lit(1)).alias("headcount"),
        F.sum("salary").alias("total_payroll"),
        F.round(F.avg("salary"), 2).alias("avg_salary")
        )
    .filter(F.col("headcount") >= 5)
    )

# To validate your solution, convert your final pySpark df to a pandas df
techcorp_workforce.toPandas()