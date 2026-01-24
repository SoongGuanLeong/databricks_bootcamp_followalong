from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName("run-pyspark-code").getOrCreate()


def etl(products, sales):
    # Write code here
    df = (
        sales.join(products, "product_id")
        .withColumn(
            "rank",
            F.row_number().over(
                W.partitionBy("category").orderBy(F.col("revenue").desc())
            ),
        )
        .select("category", "product_name", "rank", "revenue")
    )
    return df
