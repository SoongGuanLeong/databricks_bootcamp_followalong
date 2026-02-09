# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

# Start writing code
premium_accounts_by_day = (
    premium_accounts_by_day
    .withColumn("flag", F.when(F.col("final_price") == 0, F.lit(0)).otherwise(F.lit(1)))
    .select("account_id", "entry_date", "flag")
    )
    
premium_accounts_by_day_1 = (
    premium_accounts_by_day.filter(F.col("entry_date").between("2022-02-07", "2022-02-13"))
    .filter(F.col("flag") == 1)
    )

premium_accounts_by_day_2 = (
    premium_accounts_by_day
    .filter(F.col("entry_date").between("2022-02-14", "2022-02-20"))
    .withColumn("entry_date", F.date_sub(F.col("entry_date"), 7))
    .withColumnRenamed("flag", "flag_7")
    )

premium_accounts_by_day = (
    premium_accounts_by_day_1.join(premium_accounts_by_day_2, on=["account_id", "entry_date"], how="left")
    .groupBy("entry_date")
    .agg(
        F.sum(F.col("flag")).alias("premium_paid_accounts"),
        F.sum(F.col("flag_7")).alias("premium_paid_accounts_after_7d")
        )
    .orderBy("entry_date")
    )

# To validate your solution, convert your final pySpark df to a pandas df
premium_accounts_by_day.toPandas()