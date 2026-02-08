# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
airbnb_hosts = (
    airbnb_hosts.join(airbnb_guests, ["nationality", "gender"])
    .select("host_id", "guest_id")
    .distinct()
)

# To validate your solution, convert your final pySpark df to a pandas df
airbnb_hosts.toPandas()
