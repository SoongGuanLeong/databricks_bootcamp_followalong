# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
sf_restaurant_health_violations = (
    sf_restaurant_health_violations.filter(F.col("business_name") == "Roxanne Cafe")
    .filter(~F.col("violation_id").isNull())
    .withColumn("year", F.year(F.col("inspection_date")))
    .groupBy("year")
    .agg(F.count("*").alias("n_violations"))
    .orderBy("year")
)

# To validate your solution, convert your final pySpark df to a pandas df
sf_restaurant_health_violations.toPandas()
