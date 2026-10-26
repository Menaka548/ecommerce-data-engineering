# E-commerce Data Engineering Pipeline

## Project Overview
This project demonstrates an end-to-end e-commerce data processing pipeline using PySpark and Delta Lake. It processes retail transaction data, performs data cleaning and quality checks, and generates aggregated outputs for analysis.

## Architecture

**Raw CSV → Bronze → Silver → Gold**

- **Bronze Layer:** Loads raw e-commerce transaction data into Delta format.
- **Silver Layer:** Removes duplicate records and prepares cleaned data.
- **Data Quality:** Checks missing values, duplicate records, negative quantities, negative prices, and zero quantities.
- **Gold Layer:** Aggregates sales data by country and product for reporting.

## Technologies Used
- Python
- PySpark
- Spark SQL
- Delta Lake
- Git and GitHub

## Project Files
- `main.py` — Main data pipeline
- `bronze_layer.py` — Bronze layer processing
- `data_quality_checks.py` — Data quality validation
- `gold_delta_layer.py` — Gold layer Delta tables
- `delta_merge.py` — Delta Lake MERGE operations
- `delta_operations.py` — Delta Lake operations
- `delta_history.py` — Delta table history
- `delta_time_travel.py` — Delta Lake time travel
- `requirements.txt` — Python dependencies

## How to Run
1. Install Python and Java prerequisites for your PySpark setup.
2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Place the source CSV in the expected `data/raw/` directory.
4. Run the main pipeline:

   ```bash
   python main.py
   ```

## Learning Objectives
- Build a layered data pipeline.
- Clean and validate transaction data.
- Work with Delta Lake tables and operations.
- Generate country-level and product-level sales summaries.

## Author
Menaka

## Repository
https://github.com/Menaka548/ecommerce-data-engineering
