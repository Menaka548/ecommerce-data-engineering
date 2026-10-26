
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("DeltaLakeTest")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

data = [(1, "Laptop", 500), (2, "Phone", 300)]
columns = ["id", "product", "price"]

df = spark.createDataFrame(data, columns)

output_path = "data/processed/delta_test"

df.write.format("delta").mode("overwrite").save(output_path)

print("Delta Lake write successful!")

spark.read.format("delta").load(output_path).show()

spark.stop()
