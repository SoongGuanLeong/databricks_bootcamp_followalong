# Initialize Spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Spark Playground").getOrCreate()

# Copy the starter code or load the file path available in the problem statement

df = (
    spark.read.option("header", "true")
    .option("inferschema", "true")
    .csv("/datasets/customers.csv")
)

df_result = (
    df.filter(col("purchase_amount") > 100)
    .filter(col("age") >= 30)
    .select("customer_id", "name", "purchase_amount")
)
# Display the final DataFrame using the display() function.
display(df_result)
