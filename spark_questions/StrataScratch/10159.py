# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
airbnb_contacts = (
    airbnb_contacts.groupBy("id_guest")
    .agg(F.sum(F.col("n_messages")).alias("sum_n_messages"))
    .withColumn(
        "ranking", F.dense_rank().over(W.orderBy(F.col("sum_n_messages").desc()))
    )
)

# To validate your solution, convert your final pySpark df to a pandas df
airbnb_contacts.toPandas()
