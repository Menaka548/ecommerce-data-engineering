
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("SilverLayerCheck")
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
    silver_path = "data/processed/ecommerce_cleaned"

    silver_df = spark.read.format("delta").load(silver_path)

    print("Silver Layer Validation")
    print("Total rows:", silver_df.count())
    print("Total columns:", len(silver_df.columns))
    print("Columns:", silver_df.columns)

finally:
    spark.stop()
