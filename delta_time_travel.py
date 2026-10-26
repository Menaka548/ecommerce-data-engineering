
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("DeltaLakeTimeTravel")
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

table_path = "data/processed/ecommerce_cleaned"

old_df = (
    spark.read
    .format("delta")
    .option("versionAsOf", 0)
    .load(table_path)
)

print("Reading Delta table version 0:")
print("Row count:", old_df.count())

old_df.show(5, truncate=False)

spark.stop()
