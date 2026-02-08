# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
lyft_drivers = lyft_drivers.filter(
    (F.col("yearly_salary") <= 30000) | (F.col("yearly_salary") >= 70000)
)

# To validate your solution, convert your final pySpark df to a pandas df
lyft_drivers.toPandas()
