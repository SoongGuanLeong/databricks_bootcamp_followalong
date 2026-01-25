from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(aerospace_df, company_df):
	# Write code here
    aerospace_df = (
        aerospace_df.withColumnRenamed("name", "equipment_name")
            .withColumnRenamed("type", "equipment_type")
            .withColumnRenamed("status", "equipment_status")
    )

    company_df = (
        company_df.withColumnRenamed("id", "company_id")
            .withColumnRenamed("name", "company_name")
    )
 
	df = (
       aerospace_df.join(company_df, "company_id")
            .withColumn("status_label", F.when(F.col("equipment_status") == "inactive", "Inactive")
                                        .otherwise(F.when(F.col("country") == "USA", "Domestic Active")
                                                   .otherwise("Foreign Active")
                                                  )
                       )
            .drop("company_id")
    )

    return df