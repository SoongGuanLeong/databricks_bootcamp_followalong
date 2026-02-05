# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
oscar_nominees = (
    oscar_nominees.join(
        nominee_information, oscar_nominees.nominee == nominee_information.name
    )
    .orderBy(F.col("nominee"))
    .filter(F.col("winner") == True)
    .groupBy(["nominee", "top_genre"])
    .count()
    .orderBy(F.col("count").desc())
    .withColumn(
        "row_num",
        F.row_number().over(W.orderBy([F.col("count").desc(), F.col("nominee")])),
    )
    .filter(F.col("row_num") == 1)
    .select("top_genre")
)

# To validate your solution, convert your final pySpark df to a pandas df
oscar_nominees.toPandas()
