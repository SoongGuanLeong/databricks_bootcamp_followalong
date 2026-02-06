# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
airbnb_search_details = airbnb_search_details.groupBy("city", "property_type").agg(
    F.avg(F.col("bathrooms")).alias("n_bathrooms_avg"),
    F.avg(F.col("bedrooms")).alias("n_bedrooms_avg"),
)

# To validate your solution, convert your final pySpark df to a pandas df
airbnb_search_details.toPandas()
