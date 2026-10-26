from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("DeltaLakeHistory")
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

delta_path = "data/processed/ecommerce_cleaned"

try:
    delta_table = DeltaTable.forPath(spark, delta_path)

    print("Delta Lake Transaction History:")

    delta_table.history().select(
        "version",
        "timestamp",
        "operation"
    ).show(truncate=False)

finally:
    spark.stop()