from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Read silver table
silver_df = spark.read.table("retail_db.silver_sales")

print("Starting FP-Growth Market Basket Analysis...")

# Step 1: Create transactions (group items by InvoiceNo)
print("Step 1: Creating transactions...")
transactions = (
    silver_df
    .filter(F.col("Quantity") > 0)  # Only positive quantities
    .groupBy("InvoiceNo", "CustomerID", "InvoiceDate")
    .agg(
        F.collect_set("StockCode").alias("items"),
        F.count("StockCode").alias("item_count"),
        F.sum(F.col("Quantity") * F.col("UnitPrice")).alias("total_amount")
            )
    )