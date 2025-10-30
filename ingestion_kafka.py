from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMENTED OUT: Old dbfs:/FileStore paths (public DBFS root is disabled)
# raw_data_path = "dbfs:/FileStore/tables/OnlineRetail.csv"
# bronze_path   = "dbfs:/FileStore/retail_project/bronze"
# silver_path   = "dbfs:/FileStore/retail_project/silver"
# gold_path     = "dbfs:/FileStore/retail_project/gold"

# Updated paths to use Unity Catalog Volume
raw_data_path = "/Volumes/big_data_project/retail_db/retail_data/OnlineRetail.csv"
bronze_path   = "/Volumes/big_data_project/retail_db/retail_data/bronze"
silver_path   = "/Volumes/big_data_project/retail_db/retail_data/silver"
gold_path     = "/Volumes/big_data_project/retail_db/retail_data/gold"

# Define schema for the raw data