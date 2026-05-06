# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
los_angeles_restaurant_health_inspections = (
    los_angeles_restaurant_health_inspections.filter(F.col("owner_name") == "GLASSELL COFFEE SHOP LLC")
    )

# To validate your solution, convert your final pySpark df to a pandas df
los_angeles_restaurant_health_inspections.toPandas()