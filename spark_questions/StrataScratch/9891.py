# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
customers = (
    customers.join(orders, customers.id == orders.cust_id, how="left")
    .select("first_name", "last_name", "city", "order_details")
    .orderBy("first_name", "order_details")
    )

# To validate your solution, convert your final pySpark df to a pandas df
customers.toPandas()