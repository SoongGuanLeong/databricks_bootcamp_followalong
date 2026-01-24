from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(pages):
	# Write code here
	df = (
        pages.withColumn("rank", F.row_number()
                                    .over(W.partitionBy("domain")
                                    .orderBy(F.col("seo_score").desc())
                                         )
                        )
            .filter(F.col("rank") == 1)
            .drop("rank")
            .withColumnRenamed("url", "highest_seo_page")
            .withColumnRenamed("seo_score", "highest_seo_score")
            .withColumn("rank", F.row_number().over(W.orderBy(F.col("highest_seo_score").desc()
                                                             )
                                                   )
                       )
            .withColumn("overall_highest_page", F.when(F.col("rank")== 1, F.col("highest_seo_page")))
            .withColumn("overall_highest_score", F.when(F.col("rank")== 1, F.col("highest_seo_score")))
            .drop("rank")
    )

    return df