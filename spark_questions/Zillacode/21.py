from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName("run-pyspark-code").getOrCreate()


def etl(transactions):
    # Write code here
    from pyspark.sql.types import StringType

    df = transactions.withColumn(
        "previous_product",
        F.lag(F.col("product_id")).over(
            W.partitionBy("customer_id").orderBy(F.col("date"))
        ),
    ).withColumn(
        "date_and_product",
        F.concat(
            F.col("date"),
            F.lit(" "),
            F.coalesce(F.col("previous_product"), F.lit("None")),
        ),
    )

    return df
