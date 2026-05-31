# Databricks notebook source
# ==============================================================================
# SCRIPT: 02_silver_sales.py
# LAYER: SILVER (EXTERNAL)
# DESCRIPTION: Strict Schema Enforcement, Deduplication, and Z-Ordering.
# ==============================================================================

from pyspark.sql.functions import current_timestamp, col

# 0. The Engine Tuning ⚙️
# Kill the disk spill by increasing partitions for the 100GB batch load
spark.conf.set("spark.sql.shuffle.partitions", "1024")

# Enable Adaptive Query Execution (AQE) 
# If 1024 is too many, Databricks will dynamically merge the tiny ones!
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")


print("Igniting the Silver Clean Room... 🧼⚔️")

# 1. Configuration
SOURCE_TABLE = "tpcds_enterprise.bronze.store_sales"
TARGET_TABLE = "tpcds_enterprise.silver.store_sales"
ADLS_SILVER_PATH = "abfss://tpc-ds@stpraxaslakehouse.dfs.core.windows.net/data/silver_zone/store_sales"

print(f"Extracting 100GB of raw data from {SOURCE_TABLE}...")
df_raw = spark.table(SOURCE_TABLE)

# 2. THE ENTERPRISE SCHEMA ENFORCEMENT 🧬
# We NEVER trust Bronze types. We explicitly cast everything to its correct business type.
# This prevents string-to-int crashes downstream!
df_casted = df_raw \
    .withColumn("ss_sold_date_sk", col("ss_sold_date_sk").cast("integer")) \
    .withColumn("ss_item_sk", col("ss_item_sk").cast("integer")) \
    .withColumn("ss_customer_sk", col("ss_customer_sk").cast("integer")) \
    .withColumn("ss_ticket_number", col("ss_ticket_number").cast("integer")) \
    .withColumn("ss_quantity", col("ss_quantity").cast("integer")) \
    .withColumn("ss_sales_price", col("ss_sales_price").cast("decimal(7,2)")) \
    .withColumn("ss_net_paid", col("ss_net_paid").cast("decimal(7,2)"))

# 3. DEDUPLICATION (The Silent Killer) 🔪
# In TPC-DS, the combination of Item ID and Ticket Number creates a unique row.
# If the source API accidentally sent the same receipt twice, we kill it here.
print("Running massive distributed Deduplication...")
df_dedup = df_casted.dropDuplicates(["ss_item_sk", "ss_ticket_number"])

# 4. BUSINESS LOGIC VALIDATION 🛡️
# Dropping nulls is not enough. We filter out impossible business scenarios.
# You can't have negative quantities, and critical keys cannot be null.
df_clean = df_dedup \
    .filter(col("ss_quantity") > 0) \
    .filter(col("ss_sales_price").isNotNull()) \
    .dropna(subset=["ss_sold_date_sk", "ss_customer_sk", "ss_item_sk"]) \
    .withColumn("silver_ingestion_ts", current_timestamp())

print("Writing strict, clean data to the Silver Hybrid Layer... ⏳")

# 5. Hybrid External Write with Date Partitioning
df_clean.writeTo(TARGET_TABLE) \
    .option("path", ADLS_SILVER_PATH) \
    .partitionedBy("ss_sold_date_sk") \
    .tableProperty("delta.autoOptimize.optimizeWrite", "true") \
    .tableProperty("delta.autoOptimize.autoCompact", "true") \
    .createOrReplace()

print("✅ Clean Room Write Complete! Executing Z-Order Optimization... 🧠")

# 6. Z-Order Optimization for Downstream Speed
spark.sql(f"""
    OPTIMIZE {TARGET_TABLE}
    ZORDER BY (ss_item_sk, ss_customer_sk)
""")

print("Silver Forge Complete! The Fact Table is mathematically pure. ✨")