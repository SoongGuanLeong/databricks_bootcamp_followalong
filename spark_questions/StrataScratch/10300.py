# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
ms_user_dimension = (
    ms_user_dimension.join(ms_acc_dimension, "acc_id")
    .groupBy("user_id")
    .pivot("paying_customer")
    .agg(F.count("*"))
)

ms_download_facts = (
    ms_download_facts.join(ms_user_dimension, "user_id", how="left")
    .withColumnRenamed("date", "download_date")
    .groupBy("download_date")
    .agg(
        F.sum(F.col("downloads") * F.col("no")).alias("non_paying"),
        F.sum(F.col("downloads") * F.col("yes")).alias("paying"),
    )
    .filter(F.col("non_paying") > F.col("paying"))
    .orderBy("download_date")
)

# To validate your solution, convert your final pySpark df to a pandas df
ms_download_facts.toPandas()
