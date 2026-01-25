from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName("run-pyspark-code").getOrCreate()


def etl(portfolio, prices):
    # Write code here
    df = (
        portfolio.join(prices, "company")
        .groupBy(["PE_firm", "date"])
        .agg(
            F.sum(F.col("shares") * F.col("closing_price"))
            .cast("int")
            .alias("portfolio_value")
        )
    )

    return df
