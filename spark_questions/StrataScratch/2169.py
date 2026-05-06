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
    .groupBy()
    .agg(
        F.round(F.avg(F.when(F.col("phone_number").isNull(), 1).otherwise(0)), 2).alias("null_phone_ratio")
        )
    )

# To validate your solution, convert your final pySpark df to a pandas df
techcorp_workforce.toPandas()