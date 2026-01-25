from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df_sales, df_products):
	# Write code here
	df = (
        df_sales.join(df_products, "product_id")
            .groupBy(["date", "product_category"])
            .agg(
                F.sum(F.col("quantity_sold")).alias("total_quantity")
            )
    )

    return df