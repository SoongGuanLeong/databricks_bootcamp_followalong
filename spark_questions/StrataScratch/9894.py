# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
employee = (
    employee.withColumnRenamed("id", "emp_id")
    .select("emp_id", "first_name", "salary", "manager_id")
    .alias("emp")
)

manager = employee.alias("man")

employee = (
    employee.join(manager, F.col("emp.manager_id") == F.col("man.emp_id"))
    .filter(F.col("emp.salary") > F.col("man.salary"))
    .select("emp.first_name", "emp.salary")
)

# To validate your solution, convert your final pySpark df to a pandas df
employee.toPandas()
