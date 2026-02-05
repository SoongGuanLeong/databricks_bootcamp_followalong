# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
ms_employee_salary = (
    ms_employee_salary.withColumn(
        "row_num",
        F.row_number().over(W.partitionBy("id").orderBy(F.col("salary").desc())),
    )
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# To validate your solution, convert your final pySpark df to a pandas df
ms_employee_salary.toPandas()
