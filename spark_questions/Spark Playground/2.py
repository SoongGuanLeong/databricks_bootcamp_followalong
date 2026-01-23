# Initialize Spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Spark Playground").getOrCreate()

# Copy the starter code or load the file path available in the problem statement
df = (
    spark.read.option("header", "true")
    .option("inferschema", "true")
    .csv("/datasets/customers_raw.csv")
)

df_result = df.filter(col("customer_id").isNotNull()).filter(col("email").isNotNull())

# Display the final DataFrame using the display() function.
display(df_result)
