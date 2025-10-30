from pyspark.sql.functions import col, to_timestamp, trim, lower, regexp_replace


bronze_path = "/Volumes/big_data_project/retail_db/retail_data/bronze"
silver_path = "/Volumes/big_data_project/retail_db/retail_data/silver"


bronze_df = spark.read.parquet(bronze_path)


clean_df = (bronze_df
    .filter(col("Quantity") > 0)
    .dropna(subset=["CustomerID", "Description"])
    .withColumn("InvoiceDateTS", to_timestamp(col("InvoiceDate"), "dd/MM/yyyy HH:mm") )
    .withColumn("Description", lower(trim(regexp_replace(col("Description"), "[^a-zA-Z0-9\\s]", ""))))
)


clean_df.write.mode("overwrite").parquet(silver_path)
clean_df.write.mode("overwrite").saveAsTable("retail_db.silver_sales")
print("Silver saved + table retail_db.silver_sales created")