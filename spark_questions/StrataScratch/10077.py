# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
sf_bonus = (
    sf_bonus.groupBy("worker_ref_id")
    .agg(F.sum(F.col("bonus")).alias("bonus"))
    .withColumnRenamed("worker_ref_id", "id")
)

sf_employee = (
    sf_employee.join(sf_bonus, "id")
    .groupBy("employee_title", "sex")
    .agg(F.avg(F.col("salary") + F.col("bonus")).alias("avg_total_comp"))
)

# To validate your solution, convert your final pySpark df to a pandas df
sf_employee.toPandas()
