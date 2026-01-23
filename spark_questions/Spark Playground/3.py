# Initialize Spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum

spark = SparkSession.builder.appName("Spark Playground").getOrCreate()

# Copy the starter code or load the file path available in the problem statement
df = (
    spark.read.option("header", "true")
    .option("inferschema", "true")
    .csv("/datasets/customer_purchases.csv")
)

df_result = (
    df.groupBy("customer_id")
    .agg(_sum("purchase_amount").alias("total_purchase"))
    .orderBy("customer_id")
)


# Display the final DataFrame using the display() function.
display(df_result)
