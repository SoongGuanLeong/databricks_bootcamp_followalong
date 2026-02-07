# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code

customers = (
    customers.withColumnRenamed("id", "cust_id")
    .join(orders, "cust_id", how="outer")
    .withColumn(
        "flag",
        F.when(
            F.col("order_date").between("2019-02-01", "2019-03-01"), F.lit(1)
        ).otherwise(F.lit(0)),
    )
    .withColumn("overall_flag", F.max(F.col("flag")).over(W.partitionBy("cust_id")))
    .filter(F.col("overall_flag") == 0)
    .select("cust_id", "first_name")
    .distinct()
    .select("first_name")
)

# To validate your solution, convert your final pySpark df to a pandas df
customers.toPandas()
