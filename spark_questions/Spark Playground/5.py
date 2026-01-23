from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("Spark Playground").getOrCreate()

# Assume the dataframes products, sales, inventory are already initialized.
inventory = inventory.groupBy("product_id").agg(
    F.sum("stock").cast(IntegerType()).alias("total_stock")
)

df = (
    sales.join(products, "product_id", "outer")
    .join(inventory, "product_id", "outer")
    .withColumn("quantity", F.coalesce(F.col("quantity"), F.lit(0)))
    .withColumn("revenue", F.coalesce(F.col("revenue"), F.lit(0)))
    .withColumn("total_stock", F.coalesce(F.col("total_stock"), F.lit(0)))
)


# Write the logic and display the final dataframe
df_result = df.groupBy("category", "name", "product_id", "total_stock").agg(
    F.sum("quantity").cast(IntegerType()).alias("total_quantity"),
    F.sum("revenue").cast(IntegerType()).alias("total_revenue"),
)

display(df_result)
