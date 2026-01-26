# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
online_retail = (
    online_retail.filter(F.col("invoiceno").rlike(r"^[0-9]+$"))
    .withColumn("sales", F.col("quantity") * F.col("unitprice"))
    .withColumn("month", F.month("invoicedate"))
    .groupBy(["stockcode", "description", "month"])
    .agg(F.sum(F.col("sales")).alias("total_paid"))
    .withColumn(
        "row_num",
        F.row_number().over(W.partitionBy("month").orderBy(F.col("total_paid").desc())),
    )
    .filter(F.col("row_num") == 1)
    .drop("row_num", "stockcode")
)

# To validate your solution, convert your final pySpark df to a pandas df
online_retail.toPandas()
