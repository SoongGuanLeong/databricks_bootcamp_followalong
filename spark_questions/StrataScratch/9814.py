# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

from pyspark.ml.feature import RegexTokenizer

tokenizer = RegexTokenizer(
    inputCol="contents",
    outputCol="tokenized",
    pattern="\\W+",  # split on non-word chars
    toLowercase=True,
)

# Start writing code
google_file_store = (
    tokenizer.transform(google_file_store)
    .withColumn("word", F.explode(F.col("tokenized")))
    .filter(F.col("word").isin("bear", "bull"))
    .groupBy("word")
    .agg(F.count("*").alias("netry"))
)

# To validate your solution, convert your final pySpark df to a pandas df
google_file_store.toPandas()
