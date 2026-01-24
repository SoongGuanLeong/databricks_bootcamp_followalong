from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(employees, payroll):
	# Write code here
	df = (
        employees.join(payroll, "employee_id")
            .withColumn("pay", 
                        (F.least(F.col("hours_worked"), F.lit(40)) 
                        + F.greatest(F.col("hours_worked") - 40, F.lit(0)) * 1.5)
             * F.col("hourly_rate"))
    )

    return df.select("employee_id", "name", "pay", "position")