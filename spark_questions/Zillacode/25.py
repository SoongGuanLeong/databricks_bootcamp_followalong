from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(mines, extraction):
	# Write code here
	df = (
        mines.join(extraction, mines.id == extraction.mine_id)
            .groupBy(["location", "mineral"])
            .agg(
                F.sum(F.col("quantity")).alias("total_quantity")
            )
    )

    return df