# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
db_employee = (
    db_employee.join(db_dept, db_employee.department_id == db_dept.id)
    .agg(
        F.max(F.when(F.col("department") == "marketing", F.col("salary"))).alias(
            "m_max_salary"
        ),
        F.max(F.when(F.col("department") == "engineering", F.col("salary"))).alias(
            "e_max_salary"
        ),
    )
    .withColumn(
        "salary_difference", F.abs(F.col("m_max_salary") - F.col("e_max_salary"))
    )
    .select("salary_difference")
)

# To validate your solution, convert your final pySpark df to a pandas df
db_employee.toPandas()
