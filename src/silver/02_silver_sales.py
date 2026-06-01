# Databricks notebook source
# Databricks notebook source
# ==============================================================================
# SCRIPT: 02_silver_sales.py
# LAYER: SILVER (EXTERNAL)
# DESCRIPTION: Incremental Fact Load, Strict Schema Enforcement, and Upsert
# ==============================================================================

from pyspark.sql.functions import current_timestamp, col
from delta.tables import DeltaTable

# 0. The Engine Tuning ⚙️
spark.conf.set("spark.sql.shuffle.partitions", "1024")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

print("Igniting the Silver Fact Room... 🧼⚔️")

# 1. Configuration
SOURCE_TABLE = "tpcds_enterprise.bronze.store_sales"
TARGET_TABLE = "tpcds_enterprise.silver.store_sales"
ADLS_SILVER_PATH = "abfss://tpc-ds@stpraxaslakehouse.dfs.core.windows.net/data/silver_zone/store_sales"

# 2. THE HIGH WATERMARK LOGIC 🌊
print("Checking for existing High Watermark...")
table_exists = DeltaTable.isDeltaTable(spark, ADLS_SILVER_PATH)

if table_exists:
    try:
        max_ts_df = spark.sql(f"SELECT MAX(silver_ingestion_ts) FROM {TARGET_TABLE}")
        high_watermark = max_ts_df.collect()[0][0]
        if high_watermark is None:
            high_watermark = '1900-01-01 00:00:00'
    except Exception:
        high_watermark = '1900-01-01 00:00:00'
else:
    high_watermark = '1900-01-01 00:00:00'
    
print(f"High Watermark Detected: {high_watermark}")

# 3. INCREMENTAL EXTRACTION 🚀
print(f"Extracting ONLY new data from {SOURCE_TABLE}...")
df_raw = spark.table(SOURCE_TABLE).filter(col("bronze_ingestion_ts") > high_watermark)

if df_raw.isEmpty():
    print("🛑 No new sales transactions found. Skipping compute! 💸")
    dbutils.notebook.exit("No new data")

# 4. THE ENTERPRISE SCHEMA ENFORCEMENT 🧬
df_casted = df_raw \
    .withColumn("ss_sold_date_sk", col("ss_sold_date_sk").cast("integer")) \
    .withColumn("ss_item_sk", col("ss_item_sk").cast("integer")) \
    .withColumn("ss_customer_sk", col("ss_customer_sk").cast("integer")) \
    .withColumn("ss_ticket_number", col("ss_ticket_number").cast("integer")) \
    .withColumn("ss_quantity", col("ss_quantity").cast("integer")) \
    .withColumn("ss_sales_price", col("ss_sales_price").cast("decimal(7,2)")) \
    .withColumn("ss_net_paid", col("ss_net_paid").cast("decimal(7,2)"))

# 5. DEDUPLICATION & BUSINESS LOGIC 🔪
print("Running massive distributed Deduplication and Logic Validation...")
df_clean = df_casted.dropDuplicates(["ss_item_sk", "ss_ticket_number"]) \
    .filter(col("ss_quantity") > 0) \
    .filter(col("ss_sales_price").isNotNull()) \
    .dropna(subset=["ss_sold_date_sk", "ss_customer_sk", "ss_item_sk"]) \
    .withColumn("silver_ingestion_ts", current_timestamp())

print("Writing strict, clean data to the Silver Hybrid Layer... ⏳")

# 6. THE FAANG-GRADE INCREMENTAL UPSERT (MERGE) 🧬
if table_exists:
    print("Table exists! Executing Staff-Level MERGE for Incremental Upsert... 🔄")
    silver_table = DeltaTable.forPath(spark, ADLS_SILVER_PATH)
    
    silver_table.alias("target").merge(
        df_clean.alias("source"),
        "target.ss_item_sk = source.ss_item_sk AND target.ss_ticket_number = source.ss_ticket_number"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
    
    print("✅ MERGE complete! Only new or modified records were written. 🎯")

else:
    print("Table not found. Executing Day-0 Initial Load... 🚀")
    df_clean.writeTo(TARGET_TABLE) \
        .option("path", ADLS_SILVER_PATH) \
        .partitionedBy("ss_sold_date_sk") \
        .tableProperty("delta.autoOptimize.optimizeWrite", "true") \
        .tableProperty("delta.autoOptimize.autoCompact", "true") \
        .create()
    
    print("✅ Day-0 Initial Load Complete!")

print("Silver Fact Forge Complete! The Fact Table is mathematically pure. ✨")

spark.sql(f"""
    OPTIMIZE {TARGET_TABLE}
    ZORDER BY (ss_item_sk, ss_customer_sk)
""")
