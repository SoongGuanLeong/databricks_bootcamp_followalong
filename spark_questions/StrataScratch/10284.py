# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
facebook_friends_2 = facebook_friends.select("user2", "user1")

facebook_friends = facebook_friends.unionAll(facebook_friends_2)

total = facebook_friends.select("user1").distinct().count()

facebook_friends = facebook_friends.groupBy("user1").agg(
    (F.count("*") / F.lit(total) * F.lit(100)).alias("popularity")
)

# To validate your solution, convert your final pySpark df to a pandas df
facebook_friends.toPandas()
