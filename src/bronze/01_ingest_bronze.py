# Databricks notebook source
# ==============================================================================
# SCRIPT: 01_ingest_bronze.py
# LAYER: BRONZE (EXTERNAL)
# DESCRIPTION: Hybrid Ingestion. UC Governed, but physically isolated.
# ==============================================================================

print("Igniting the 16-Core Hybrid Bronze Engine... 🚀")

# Configuration
SOURCE_SCHEMA = "samples.tpcds_sf1000"
TARGET_SCHEMA = "tpcds_enterprise.bronze"
TABLES_TO_INGEST = ["store_sales", "customer", "item", "date_dim"]

# The Physical Azure Vault
ADLS_BRONZE_PATH = "abfss://tpc-ds@stpraxaslakehouse.dfs.core.windows.net/data/bronze_zone"

# Production Execution Loop
for table in TABLES_TO_INGEST:
    print(f"Extracting {table} from the massive Databricks catalog... ⏳")
    
    # 1. Read the raw source
    df_raw = spark.table(f"{SOURCE_SCHEMA}.{table}")
    target_table_name = f"{TARGET_SCHEMA}.{table}"
    physical_table_path = f"{ADLS_BRONZE_PATH}/{table}"
    
    # 2. THE HYBRID UPGRADE 🧬
    # By adding the 'path' option, UC creates an EXTERNAL table. 
    # If UC dies, your data is perfectly safe in your ADLS tenant!
    df_raw.writeTo(target_table_name) \
        .option("path", physical_table_path) \
        .tableProperty("delta.autoOptimize.optimizeWrite", "true") \
        .tableProperty("delta.autoOptimize.autoCompact", "true") \
        .createOrReplace()
        
    print(f"✅ {table} secured! External Table registered at {physical_table_path} 🛡️")

print("Hybrid Bronze Layer Complete! Zero Vendor Lock-in achieved. ✨")