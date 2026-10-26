
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql import functions as F

builder = (
    SparkSession.builder
    .appName("MissingDescriptionCheck")
    .master("local[2]")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

try:
    df = spark.read.format("delta").load(
        "data/processed/ecommerce_cleaned"
    )

    missing_df = df.filter(F.col("Description").isNull())

    print("Missing Description rows:", missing_df.count())

    missing_df.groupBy("StockCode", "Country").count() \
        .orderBy(F.desc("count")) \
        .show(10, truncate=False)

finally:
    spark.stop()
