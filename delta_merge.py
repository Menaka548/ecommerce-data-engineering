from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import os
import sys

python_exe = sys.executable
os.environ["PYSPARK_PYTHON"] = python_exe
os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe

builder = (
    SparkSession.builder
    .master("local[1]")
    .appName("DeltaMergePractice")
    .config("spark.driver.host", "127.0.0.1")
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
    source_data = [
        (101, "Laptop", 750.00),
        (102, "Keyboard", 45.00),
        (103, "Mouse", 25.00),
    ]

    columns = ["Product_ID", "Product_Name", "Price"]

    source_df = spark.createDataFrame(source_data, columns)

    print("Source Data:")
    source_df.show()
    # New data to merge into the Delta table
    updates = [
    (101, "Laptop", 800.0),
    (104, "Monitor", 150.0)
    ]

    updates_df = spark.createDataFrame(
    updates,
    ["Product_ID", "Product_Name", "Price"]
    )

    print("Source data for MERGE:")
    updates_df.show()

    
    # Load the existing practice Delta table
    delta_table = DeltaTable.forPath(
    spark,
    "data/processed/delta_merge_practice"
   )

    # Merge source data into the target table
    (
    delta_table.alias("target")
    .merge(
        updates_df.alias("source"),
        "target.Product_ID = source.Product_ID"
    )
    .whenMatchedUpdate(set={
        "Product_Name": "source.Product_Name",
        "Price": "source.Price"
    })
    .whenNotMatchedInsert(values={
        "Product_ID": "source.Product_ID",
        "Product_Name": "source.Product_Name",
        "Price": "source.Price"
    })
    .execute()
    )

    print("MERGE completed successfully!")
    
    final_df = spark.read.format("delta").load(
    "data/processed/delta_merge_practice"
    )

    print("Final Delta table after MERGE:")
    final_df.orderBy("Product_ID").show()
    # Create a separate practice Delta table
    practice_path = "data/processed/delta_merge_practice"

    source_df.write.format("delta").mode("overwrite").save(practice_path)

    print("Practice Delta table created successfully!")

    # Read and display the Delta table
    practice_df = spark.read.format("delta").load(practice_path)

    practice_df.show()
finally:
    spark.stop()