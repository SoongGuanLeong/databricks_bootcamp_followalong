# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code

facebook_posts = (
    facebook_posts.withColumn(
        "flag",
        F.when(F.col("post_keywords").rlike("spam"), F.lit(1)).otherwise(F.lit(0)),
    )
    .join(facebook_post_views, "post_id")
    .groupBy("post_date")
    .agg((F.sum(F.col("flag")) / F.count("*") * F.lit(100)).alias("spam_share"))
)

# To validate your solution, convert your final pySpark df to a pandas df
facebook_posts.toPandas()
