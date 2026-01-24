from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName("run-pyspark-code").getOrCreate()


def etl(MortgageDetails, UserMortgages):
    # Write code here
    # keep in mind that calculation is unnecessary here, coding just for practice

    df = (
        MortgageDetails.join(UserMortgages, "MortgageID")
        .groupBy("MortgageType")
        .agg(
            (F.sum(F.col("InterestRate")) / F.count(F.col("UserID"))).alias(
                "RateOfMortgage"
            )
        )
    )

    return df
