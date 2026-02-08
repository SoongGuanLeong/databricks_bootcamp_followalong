# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
playbook_events = (
    playbook_events.filter(F.col("device") == "macbook pro")
    .groupBy("event_name")
    .agg(F.count("*").alias("event_count"))
    .orderBy(F.col("event_count").desc())
)

# To validate your solution, convert your final pySpark df to a pandas df
playbook_events.toPandas()
