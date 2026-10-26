
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("EcommerceBronzeLayer")
    .master("local[*]")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Read original raw CSV
raw_path = "data/raw/online_retail_II (1).csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(raw_path)
)
df = df.withColumnRenamed("Customer ID", "Customer_ID")
print("Raw row count:", df.count())

# Save as a separate Bronze Delta table
bronze_path = "data/processed/bronze_ecommerce"

df.write \
    .format("delta") \
    .mode("errorifexists") \
    .save(bronze_path)

print("Bronze Delta table created successfully!")

# Verify saved data
bronze_df = spark.read.format("delta").load(bronze_path)

print("Bronze row count:", bronze_df.count())
bronze_df.show(5, truncate=False)

spark.stop()
