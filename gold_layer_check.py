
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("GoldLayerCheck")
    .getOrCreate()
)

try:
    country_path = "data/processed/gold_country_sales"
    product_path = "data/processed/gold_product_sales"

    country_df = spark.read.option("header", True).csv(country_path)
    product_df = spark.read.option("header", True).csv(product_path)

    print("=== GOLD LAYER VALIDATION ===")

    print("\nCountry Sales")
    print("Rows:", country_df.count())
    country_df.show(5, truncate=False)

    print("\nProduct Sales")
    print("Rows:", product_df.count())
    product_df.show(5, truncate=False)

finally:
    spark.stop()
