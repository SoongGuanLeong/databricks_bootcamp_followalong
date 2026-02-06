# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
linkedin_projects = linkedin_projects.withColumn(
    "duration", F.datediff(F.col("end_date"), F.col("start_date"))
).withColumnRenamed("id", "project_id")

linkedin_employees = linkedin_employees.withColumnRenamed("id", "emp_id")

linkedin_emp_projects = (
    linkedin_emp_projects.join(linkedin_projects, "project_id")
    .join(linkedin_employees, "emp_id")
    .groupBy("title", "budget")
    .agg(
        F.ceil(F.sum(F.col("duration") * F.col("salary")) / F.lit(365)).alias(
            "prorated_expense"
        )
    )
    .filter(F.col("budget") < F.col("prorated_expense"))
)

# To validate your solution, convert your final pySpark df to a pandas df
linkedin_emp_projects.toPandas()
