# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
car_launches = (
    car_launches.filter(F.col("year").isin([2019, 2020]))
    .groupBy("company_name")
    .agg(
        F.count_distinct(F.when(F.col("year") == 2019, F.col("product_name"))).alias(
            "cnt_2019"
        ),
        F.count_distinct(F.when(F.col("year") == 2020, F.col("product_name"))).alias(
            "cnt_2020"
        ),
    )
    .withColumn("net_products", F.col("cnt_2020") - F.col("cnt_2019"))
    .select("company_name", "net_products")
)

# To validate your solution, convert your final pySpark df to a pandas df
car_launches.toPandas()
