from pyspark.sql.functions import round, sum as _sum, month, year

# Read silver table
silver_df = spark.read.table("retail_db.silver_sales")

# Create gold dataframe with calculated TotalAmount
gold_df = silver_df.withColumn("TotalAmount", round(silver_df.Quantity * silver_df.UnitPrice, 2))

# Calculate country sales aggregation
country_sales = gold_df.groupBy("Country").agg(_sum("TotalAmount").alias("TotalSales")).orderBy("TotalSales", ascending=False)

# Calculate top products by quantity
top_products = gold_df.groupBy("StockCode", "Description").agg(_sum("Quantity").alias("TotalQty")).orderBy("TotalQty", ascending=False)

# UPDATED: Use Unity Catalog Volumes instead of restricted DBFS root paths
# OLD (COMMENTED OUT): country_sales.write.mode("overwrite").format("delta").save("/mnt/data/retail_project/data/gold/revenue_by_country")
# OLD (COMMENTED OUT): top_products.write.mode("overwrite").format("delta").save("/mnt/data/retail_project/data/gold/top_products")

# NEW: Write to Unity Catalog Volumes
country_sales.write.mode("overwrite").format("delta").save("/Volumes/big_data_project/retail_db/retail_data/gold/revenue_by_country")
top_products.write.mode("overwrite").format("delta").save("/Volumes/big_data_project/retail_db/retail_data/gold/top_products")

# Also write to managed tables
country_sales.write.mode("overwrite").saveAsTable("retail_db.gold_country_sales")
top_products.write.mode("overwrite").saveAsTable("retail_db.gold_top_products")

print("Gold tables created: gold_country_sales, gold_top_products")
print("Gold data written to Unity Catalog Volumes: /Volumes/big_data_project/retail_db/retail_data/gold/")