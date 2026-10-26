from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("EcommerceDataEngineering") \
    .master("local[*]") \
    .getOrCreate()

print("Spark is running successfully!")

spark.stop()