# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
customers = customers.withColumnRenamed("id", "cust_id")

customers = (
    orders.join(customers, "cust_id")
    .filter(F.col("first_name").isin("Jill", "Eva"))
    .orderBy("cust_id")
    .select("first_name", "order_date", "order_details", "total_order_cost")
)


# To validate your solution, convert your final pySpark df to a pandas df
customers.toPandas()
