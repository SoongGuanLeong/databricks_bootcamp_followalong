# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
techcorp_workforce = (
    techcorp_workforce
    .filter(F.col("department").isin(["HR", "Admin"]))
    .filter(F.col("salary") > 80000)
    .select("first_name", "last_name", "department", "salary")
    )

# To validate your solution, convert your final pySpark df to a pandas df
techcorp_workforce.toPandas()