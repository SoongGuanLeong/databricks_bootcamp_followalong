# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
worker = (
    title.join(worker, title.worker_ref_id == worker.worker_id, how="left")
    .withColumn("rnk", F.rank().over(W.orderBy(F.col("salary").desc())))
    .filter(F.col("rnk") == 1)
    .withColumnRenamed("worker_title", "best_paid_title")
    .select("best_paid_title")
    .distinct()
)

# To validate your solution, convert your final pySpark df to a pandas df
worker.toPandas()
