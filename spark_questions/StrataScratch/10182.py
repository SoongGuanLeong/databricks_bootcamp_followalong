# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
sf_restaurant_health_violations = (
    sf_restaurant_health_violations.select("business_postal_code", "business_address")
    .withColumnRenamed("business_postal_code", "postal_code")
    .withColumn("normalized", F.upper(F.trim(F.col("business_address"))))
    .withColumn(
        "street",
        F.when(
            F.col("normalized").rlike("^[0-9]"),
            F.regexp_extract(F.col("normalized"), r"^[0-9]+\s+([^\s]+)", 1),
        ).otherwise(F.regexp_extract(F.col("business_address"), r"^([^\s]+)", 1)),
    )
    .select("postal_code", "street")
    .distinct()
    .filter(~F.col("postal_code").isNull())
    .groupBy("postal_code")
    .agg(F.count("*").alias("n_streets"))
    .orderBy(F.col("n_streets").desc(), F.col("postal_code"))
)

# To validate your solution, convert your final pySpark df to a pandas df
sf_restaurant_health_violations.toPandas()
