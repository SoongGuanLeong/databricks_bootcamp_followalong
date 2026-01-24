from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(projects, employees, equipment):
	# Write code here
	projects = (
        projects.withColumn("duration_days", F.datediff(F.col("end_date"), F.col("start_date")))
            .select("project_id", "project_name", "start_date", "end_date", "duration_days")
    )
        

    employees = (
        employees.groupBy("project_id")
            .agg(
                F.count_distinct(F.col("employee_id")).alias("total_employees"),
                F.count_distinct(F.col("role")).alias("unique_roles")
            )
    )

    equipment = (
        equipment.groupBy("project_id")
            .agg(
                F.sum(F.col("cost")).alias("total_equipment_cost")
            )
    )

    df = (
        projects.join(employees, "project_id", "outer")
            .join(equipment, "project_id", "outer")
        )

    return df