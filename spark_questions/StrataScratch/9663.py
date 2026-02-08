# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
forbes_global_2010_2014 = (
    forbes_global_2010_2014.filter(F.col("sector") == "Financials")
    .withColumn("rnk", F.rank().over(W.orderBy(F.col("profits").desc())))
    .filter(F.col("rnk") == 1)
    .select("company", "continent")
)

# To validate your solution, convert your final pySpark df to a pandas df
forbes_global_2010_2014.toPandas()
