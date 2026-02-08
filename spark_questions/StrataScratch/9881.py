# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
titanic = (
    titanic.groupBy("survived")
    .pivot("pclass")
    .agg(F.count("*"))
    .withColumnRenamed("1", "first_class")
    .withColumnRenamed("2", "second_class")
    .withColumnRenamed("3", "third_class")
)

# To validate your solution, convert your final pySpark df to a pandas df
titanic.toPandas()
