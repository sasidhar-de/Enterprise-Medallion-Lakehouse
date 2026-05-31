# Databricks notebook source
# ==============================================================================
# SCRIPT: 02_silver_dimensions.py
# LAYER: SILVER (EXTERNAL)
# DESCRIPTION: Dynamic loop to cleanse and load all Dimension tables.
# ==============================================================================

from pyspark.sql.functions import current_timestamp, col

print("Igniting the Silver Dimension Forge... 🛠️✨")

# 1. The Configuration Matrix
# We map the table name to its strict Primary Key for automated deduplication.
DIMENSIONS = {
    "customer": "c_customer_sk",
    "item": "i_item_sk",
    "date_dim": "d_date_sk"
}

SOURCE_DATABASE = "tpcds_enterprise.bronze"
TARGET_DATABASE = "tpcds_enterprise.silver"
ADLS_SILVER_BASE = "abfss://tpc-ds@stpraxaslakehouse.dfs.core.windows.net/data/silver_zone"

# 2. The Dynamic Execution Loop
for table_name, pk_column in DIMENSIONS.items():
    print(f"\nProcessing Dimension: {table_name.upper()} ⏳")
    
    source_table = f"{SOURCE_DATABASE}.{table_name}"
    target_table = f"{TARGET_DATABASE}.{table_name}"
    physical_path = f"{ADLS_SILVER_BASE}/{table_name}"
    
    # Extract
    df_raw = spark.table(source_table)
    
    # 3. The Clean Room Transformation 🧼
    # - Cast the Primary Key strictly to Integer
    # - Drop any row where the Primary Key is missing (corrupt data)
    # - Deduplicate strictly based on the Primary Key
    df_clean = df_raw \
        .withColumn(pk_column, col(pk_column).cast("integer")) \
        .dropna(subset=[pk_column]) \
        .dropDuplicates([pk_column]) \
        .withColumn("silver_ingestion_ts", current_timestamp())
        
    print(f"Writing {table_name} to External Silver Layer...")
    
    # 4. The Hybrid Write (No Partitioning for Dimensions!)
    df_clean.writeTo(target_table) \
        .option("path", physical_path) \
        .tableProperty("delta.autoOptimize.optimizeWrite", "true") \
        .tableProperty("delta.autoOptimize.autoCompact", "true") \
        .createOrReplace()
        
    print(f"✅ {table_name} perfectly cleansed and stored at {physical_path}! 🛡️")

print("\nAll Dimensions Processed! The Silver Layer is 100% Complete. 🏆")