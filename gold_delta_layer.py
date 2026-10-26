
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("GoldDeltaLayer")
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

try:
    # Existing Gold CSV outputs
    country_csv = "data/processed/gold_country_sales"
    product_csv = "data/processed/gold_product_sales"

    # New Gold Delta tables
    country_delta = "data/processed/gold_country_sales_delta"
    product_delta = "data/processed/gold_product_sales_delta"

    # Read existing CSV data
    country_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(country_csv)
    )

    product_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(product_csv)
    )

    # Save as Delta tables without overwriting existing data
    country_df.write.format("delta").mode("errorifexists").save(country_delta)

    product_df.write.format("delta").mode("errorifexists").save(product_delta)

    # Validate the new Delta tables
    country_gold = spark.read.format("delta").load(country_delta)
    product_gold = spark.read.format("delta").load(product_delta)

    print("=== GOLD DELTA LAYER VALIDATION ===")
    print("Country sales rows:", country_gold.count())
    print("Product sales rows:", product_gold.count())

    print("\nCountry sales sample:")
    country_gold.show(5, truncate=False)

    print("\nProduct sales sample:")
    product_gold.show(5, truncate=False)

    print("\nGold Delta tables created and validated successfully!")

finally:
    spark.stop()
