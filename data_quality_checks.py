

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import csv
from pathlib import Path

builder = (
    SparkSession.builder
    .appName("DataQualityChecks")
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

df = spark.read.format("delta").load(
    "data/processed/ecommerce_cleaned"
)
df.printSchema()
print("Total rows:", df.count())

print(
    "Missing Invoice:",
    df.filter(df["Invoice"].isNull()).count()
)
missing_invoice = df.filter(
    df["Invoice"].isNull()
).count()

if missing_invoice == 0:
    print("PASS: Invoice has no missing values")
else:
    print("FAIL: Invoice has missing values")

missing_stockcode = df.filter(
    df["StockCode"].isNull()
).count()

if missing_stockcode == 0:
    print("PASS: StockCode has no missing values")
else:
    print("FAIL: StockCode has missing values")

missing_description = df.filter(
    df["Description"].isNull()
).count()

if missing_description == 0:
    print("PASS: Description has no missing values")
else:
    print(
        f"FAIL: Description has {missing_description} missing values"
    )

# Inspect records with missing descriptions
print("\n===== MISSING DESCRIPTION ANALYSIS =====")

df.filter(
    df["Description"].isNull()
).select(
    "Invoice", "StockCode", "Description", "Quantity", "Price", "Country"
).show(10, truncate=False)    

# Check missing values in important columns
print("\n===== ADDITIONAL NULL CHECKS =====")

for column in ["Customer_ID", "InvoiceDate", "Quantity", "Price"]:
    null_count = df.filter(df[column].isNull()).count()
    print(f"{column} missing values: {null_count}")

# Inspect negative price records
print("\n===== NEGATIVE PRICE ANALYSIS =====")

df.filter(
    df["Price"] < 0
).select(
    "Invoice",
    "StockCode",
    "Description",
    "Quantity",
    "Price",
    "Country"
).show(10, truncate=False)

# Check missing descriptions by price
print("\n===== MISSING DESCRIPTION PRICE ANALYSIS =====")

df.filter(
    df["Description"].isNull()
).groupBy(
    "Price"
).count().orderBy(
    "Price"
).show()

zero_quantity = df.filter(
    df["Quantity"] == 0
).count()

if zero_quantity == 0:
    print("PASS: No zero-quantity records")
else:
    print(
        f"FAIL: Found {zero_quantity} zero-quantity records"
    )

negative_price = df.filter(
    df["Price"] < 0
).count()

if negative_price == 0:
    print("PASS: No negative prices")
else:
    print(
        f"FAIL: Found {negative_price} negative prices"
    )

print("\n===== MISSING DESCRIPTION SUMMARY =====")

df.filter(
    df["Description"].isNull()
).groupBy(
    "StockCode",
    "Country"
).count().orderBy(
    "count",
    ascending=False
).show(10, truncate=False)


# Check duplicate records
duplicate_count = df.count() - df.dropDuplicates().count()

print(f"Duplicate records: {duplicate_count}")

print("\n===== DATA QUALITY SUMMARY =====")
# Check future invoice dates
from pyspark.sql import functions as F

future_date_count = df.filter(
    F.col("InvoiceDate") > F.current_timestamp()
).count()

print(f"Future invoice dates: {future_date_count}")
checks = [
    ("Invoice missing values", missing_invoice == 0),
    ("StockCode missing values", missing_stockcode == 0),
    ("Description missing values", missing_description == 0),
    ("Zero quantity records", zero_quantity == 0),
    ("Negative prices", negative_price == 0),
    ("Duplicate records", duplicate_count == 0),
    ("Future invoice dates", future_date_count == 0),
]
for column in ["Customer_ID", "InvoiceDate", "Quantity", "Price"]:
    null_count = df.filter(df[column].isNull()).count()
    checks.append((f"{column} missing values", null_count == 0))

passed = sum(status for _, status in checks)
failed = len(checks) - passed

for name, status in checks:
    result = "PASS" if status else "FAIL"
    print(f"{result}: {name}")

print(f"\nTotal checks: {len(checks)}")
print(f"Passed: {passed}")
print(f"Failed: {failed}")

# Save data quality report as CSV
report_path = Path("data/processed/data_quality_report.csv")
report_path.parent.mkdir(parents=True, exist_ok=True)

with report_path.open("w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)

    writer.writerow(["Check Name", "Status"])

    for name, status in checks:
        writer.writerow([name, "PASS" if status else "FAIL"])

    writer.writerow(["Total Checks", len(checks)])
    writer.writerow(["Passed", passed])
    writer.writerow(["Failed", failed])

print(f"Data quality report saved to: {report_path}")
spark.stop()
