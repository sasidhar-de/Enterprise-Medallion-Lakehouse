# Databricks notebook source
# Databricks notebook source
# ==============================================================================
# SCRIPT: 01_ingest_bronze.py
# LAYER: BRONZE (EXTERNAL)
# DESCRIPTION: Immutable Append-Only Ledger with Audit Tracking & UC Governance.
# ==============================================================================

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

print("Igniting the 16-Core Hybrid Bronze Engine... 🚀")

# Configuration
SOURCE_SCHEMA = "samples.tpcds_sf1"
TARGET_SCHEMA = "tpcds_enterprise.bronze"
TABLES_TO_INGEST = ["store_sales", "customer", "item", "date_dim"]

# The Physical Azure Vault
ADLS_BRONZE_PATH = "abfss://tpc-ds@stpraxaslakehouse.dfs.core.windows.net/data/bronze_zone"

# Production Execution Loop
for table in TABLES_TO_INGEST:
    print(f"\n========================================")
    print(f"Extracting {table.upper()} from Source... ⏳")
    print(f"========================================")
    
    # 1. Read the raw source
    df_raw = spark.table(f"{SOURCE_SCHEMA}.{table}")
    target_table_name = f"{TARGET_SCHEMA}.{table}"
    physical_table_path = f"{ADLS_BRONZE_PATH}/{table}"
    
    # 2. THE AUDIT INJECTION 🧬
    # Stamping the exact millisecond to create our High Watermark for Silver
    df_audited = df_raw.withColumn("bronze_ingestion_ts", current_timestamp())
    
    # 3. THE SMART SWITCH (Append-Only Ledger) 🧠
    table_exists = DeltaTable.isDeltaTable(spark, physical_table_path)
    
    if not table_exists:
        print(f"Table not found. Executing Day-0 Initial Vault Build... 🏗️")
        df_audited.writeTo(target_table_name) \
            .option("path", physical_table_path) \
            .tableProperty("delta.autoOptimize.optimizeWrite", "true") \
            .tableProperty("delta.autoOptimize.autoCompact", "true") \
            .create()
        print(f"✅ Day-0 Build Complete for {table}!")
        
    else:
        print(f"Vault exists! Executing Day-N Append to historical ledger... 📥")
        # For existing UC external tables, saveAsTable with append mode is the cleanest API
        # ====================================================================
        # 🩹 THE SELF-HEALING POINTER (Place it right here!)
        # Rebuilds the Unity Catalog pointer if someone accidentally dropped it
        # ====================================================================
        spark.sql(f"CREATE TABLE IF NOT EXISTS {target_table_name} USING DELTA LOCATION '{physical_table_path}'")
        df_audited.write.format("delta").mode("append").saveAsTable(target_table_name)
        print(f"✅ New batch safely appended to {table}!")

print("\nHybrid Bronze Layer Complete! The historical ledger is secure. ✨")


