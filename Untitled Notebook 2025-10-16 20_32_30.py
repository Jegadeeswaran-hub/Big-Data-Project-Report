# Databricks notebook source
# Databricks notebook: main_pipeline


# ----------------------------------------
# Step 1: Set Environment
# ----------------------------------------
print("🚀 Starting Big Data Retail Pipeline...")
print("Loading Spark and Hive Environment...")


spark.sql("CREATE DATABASE IF NOT EXISTS retail_db")
spark.sql("USE retail_db")


# ----------------------------------------
# Step 2: Ingestion via Kafka or CSV
# ----------------------------------------
print("📥 Running Data Ingestion...")
%run ./scripts/ingestion_kafka


# ----------------------------------------
# Step 3: Data Cleaning & Transformation
# ----------------------------------------
print("🧹 Running Data Cleaning (Silver Layer)...")
%run ./scripts/spark_cleaning


# ----------------------------------------
# Step 4: Batch KPIs Calculation (Gold Layer)
# ----------------------------------------
print("📊 Running Batch KPI Computations...")
%run ./scripts/spark_batch_kpi


# ----------------------------------------
# Step 5: Market Basket Analysis (FP-Growth)
# ----------------------------------------
print("🧠 Running FP-Growth Market Basket Analysis...")
%run ./scripts/fp_growth_analysis


# ----------------------------------------
# Step 6: Streaming Analytics
# ----------------------------------------
print("🔄 Starting Streaming Analytics...")
%run ./scripts/spark_streaming


# ----------------------------------------
# Step 7: Summary
# ----------------------------------------
print("✅ Pipeline Execution Completed Successfully!")


# Optional: Verify Hive Tables
# Verify Hive Tables
display(
    spark.sql("SHOW TABLES IN retail_db")
)

# Copy file: specify both source and destination
dbutils.fs.cp(
    "/Volumes/main_catalog/retail_schema/retail_project/OnlineRetail.csv",
    "/tmp/OnlineRetail.csv"
)


# COMMAND ----------

df = spark.read.csv("/Volumes/main_catalog/retail_schema/retail_project/OnlineRetail.csv", header=True, inferSchema=True)
display(df)


# COMMAND ----------

# Bronze Data Ingestion
# Read OnlineRetail.csv from volume and write to bronze directory

print("📥 Starting Bronze Data Ingestion...")

# Source path (from main_catalog volume)
source_path = "/Volumes/main_catalog/retail_schema/retail_project/OnlineRetail.csv"

# Target bronze path (in big_data_project catalog)
bronze_path = "/Volumes/big_data_project/retail_db/retail_data/bronze"

# Read CSV data
print(f"Reading data from: {source_path}")
df = spark.read.csv(source_path, header=True, inferSchema=True)

# Show record count
record_count = df.count()
print(f"Total records read: {record_count:,}")

# Write as parquet to bronze directory
print(f"Writing bronze data to: {bronze_path}")
df.write.mode("overwrite").parquet(bronze_path)

print("✅ Bronze data ingestion completed successfully!")
print(f"Bronze data location: {bronze_path}")

# Verify the data was written
verify_df = spark.read.parquet(bronze_path)
verify_count = verify_df.count()
print(f"\nVerification: {verify_count:,} records in bronze directory")
display(verify_df.limit(10))

# COMMAND ----------

# fp_growth_analysis.py
from pyspark.sql.functions import collect_set, size
from pyspark.ml.fpm import FPGrowth

silver_df = spark.read.table("retail_db.silver_sales")

transactions = (
    silver_df.groupBy("InvoiceNo")
    .agg(collect_set("Description").alias("items"))
    .filter(size("items") > 1)
)

# Optionally filter very rare items to reduce noise
# item_freq = transactions.select(F.explode("items").alias("item")).groupBy("item").count()
# keep_items = item_freq.filter("count >= 5").select("item")  # example threshold

# Removed the problematic spark.conf.set(...) line

fp = FPGrowth(
    minSupport=0.005,
    minConfidence=0.2,
    itemsCol="items"
)

model = fp.fit(transactions)

gold_fp_base = "dbfs:/FileStore/retail_project/gold/fp_growth"
model.freqItemsets.write.mode("overwrite").parquet(gold_fp_base + "/freq_itemsets")
model.associationRules.write.mode("overwrite").parquet(gold_fp_base + "/assoc_rules")

# Save model metadata (optional)
# model.write().overwrite().save(gold_fp_base + "/model")

print("FP-Growth complete. Itemsets & rules saved.")