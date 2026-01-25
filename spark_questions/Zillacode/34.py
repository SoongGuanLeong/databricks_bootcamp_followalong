from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName("run-pyspark-code").getOrCreate()


def etl(page_visits, page_likes, page_comments):
    # Write code here
    page_visits = (
        page_visits.withColumnRenamed("visit_time", "interaction_time")
        .withColumn("interaction_type", F.lit("visit"))
        .select("interaction_time", "interaction_type", "page_id", "user_id")
    )
    page_likes = (
        page_likes.withColumnRenamed("like_time", "interaction_time")
        .withColumn("interaction_type", F.lit("like"))
        .select("interaction_time", "interaction_type", "page_id", "user_id")
    )
    page_comments = (
        page_comments.withColumnRenamed("comment_time", "interaction_time")
        .withColumn("interaction_type", F.lit("comment"))
        .select("interaction_time", "interaction_type", "page_id", "user_id")
    )

    df = page_visits.union(page_likes).union(page_comments)

    return df
