# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
airbnb_hosts = (
    airbnb_units.join(airbnb_hosts, "host_id", how="left")
    .distinct()
    .filter(F.col("age") < 30)
    .filter(F.col("unit_type") == "Apartment")
    .groupBy("host_id", "nationality")
    .agg(F.count(F.col("unit_id")).alias("apartment_count"))
    .groupBy("nationality")
    .agg(F.max(F.col("apartment_count")).alias("apartment_count"))
    .orderBy(F.col("apartment_count").desc())
)

# To validate your solution, convert your final pySpark df to a pandas df
airbnb_hosts.toPandas()
