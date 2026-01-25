from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df1, df2):
	# Write code here
	df2 = (
        df2.withColumn("maintenance_cost_rank", 
                       F.dense_rank().over(W.partitionBy("equipment_id").orderBy(F.col("maintenance_cost").desc())))
            .withColumn("latest_maintenance_date", 
                        F.max(F.col("maintenance_date")).over(W.partitionBy("equipment_id")))
            .filter(F.col("maintenance_date") == F.col("latest_maintenance_date"))
            .select("equipment_id", "latest_maintenance_date", "maintenance_cost_rank")
    )

    df = df2.join(df1, "equipment_id")

    return df