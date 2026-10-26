from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("DeltaLakeHistory")
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

delta_table = DeltaTable.forPath(spark, table_path)

delta_table.history().show(truncate=False)

spark.stop()