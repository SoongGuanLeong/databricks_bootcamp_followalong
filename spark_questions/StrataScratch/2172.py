# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
online_store_customers = (
    online_store_orders.filter(F.col("amount") > 100)
    .join(online_store_customers, on="customer_id", how="left")
    .select("customer_id", "customer_name").distinct()
    )

# To validate your solution, convert your final pySpark df to a pandas df
online_store_customers.toPandas()