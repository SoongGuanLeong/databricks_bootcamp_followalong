# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
amazon_shipment = (
    amazon_shipment.withColumn(
        "year_month", F.date_format(F.col("shipment_date"), "yyyy-MM")
    )
    .select("year_month", "shipment_id", "sub_id")
    .distinct()
    .groupBy("year_month")
    .agg(F.count("*").alias("count"))
)

# To validate your solution, convert your final pySpark df to a pandas df
amazon_shipment.toPandas()
