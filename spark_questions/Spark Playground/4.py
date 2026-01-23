from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName("Spark Playground").getOrCreate()

# Assume the dataframes employees, payroll are already initialized.
df_result = (
    employees.join(payroll, "employee_id")
    .withColumn(
        "OT",
        F.when(F.col("hours_worked") > 40, F.col("hours_worked") - 40).otherwise(0),
    )
    .withColumn(
        "NOT", F.when(F.col("hours_worked") > 40, 40).otherwise(F.col("hours_worked"))
    )
    .withColumn("pay", F.col("hourly_rate") * (F.col("NOT") + 1.5 * F.col("OT")))
    .select("employee_id", "name", "pay", "position")
)


# Write the logic and display the final dataframe

display(df_result)
