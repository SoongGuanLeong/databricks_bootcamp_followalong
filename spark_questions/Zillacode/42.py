from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(AnimalData, RegionData):
	# Write code here
    
    
	AnimalData = (
        AnimalData.groupBy("Species", "Region")
            .agg(
                F.avg(F.col("age")).alias("AvgAge"),
                F.avg(F.col("Weight")).cast("int").alias("AvgWeight"),
                F.count_distinct(F.col("ID")).alias("TotalAnimals")
            )
    )

    df = AnimalData.join(RegionData, "Region")

    return df.select("AvgAge", "AvgWeight", "Climate", "Species", "TotalAnimals")