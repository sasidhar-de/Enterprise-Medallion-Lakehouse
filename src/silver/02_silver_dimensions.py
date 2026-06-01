# Databricks notebook source
# Databricks notebook source
# ==============================================================================
# SCRIPT: 02_silver_dimensions.py
# LAYER: SILVER (EXTERNAL)
# DESCRIPTION: Incremental SCD Type 2 Dimension Load (Clean Architecture)
# ==============================================================================

from pyspark.sql.functions import current_timestamp, col, lit
from delta.tables import DeltaTable

print("Igniting the Silver Dimension Forge... 🛠️✨")

DIMENSIONS = {
    "customer": "c_customer_sk",
    "item": "i_item_sk",
    "date_dim": "d_date_sk"
}

SOURCE_DATABASE = "tpcds_enterprise.bronze"
TARGET_DATABASE = "tpcds_enterprise.silver"
ADLS_SILVER_BASE = "abfss://tpc-ds@stpraxaslakehouse.dfs.core.windows.net/data/silver_zone"

for table_name, pk_column in DIMENSIONS.items():
    print(f"\n========================================")
    print(f"Processing Dimension: {table_name.upper()} ⏳")
    print(f"========================================")
    
    source_table = f"{SOURCE_DATABASE}.{table_name}"
    target_table = f"{TARGET_DATABASE}.{table_name}"
    physical_path = f"{ADLS_SILVER_BASE}/{table_name}"
    
    table_exists = DeltaTable.isDeltaTable(spark, physical_path)
    
    # 1. THE HIGH WATERMARK LOGIC 🌊
    if table_exists:
        try:
            max_ts_df = spark.sql(f"SELECT MAX(silver_ingestion_ts) FROM {target_table}")
            high_watermark = max_ts_df.collect()[0][0]
            if high_watermark is None:
                high_watermark = '1900-01-01 00:00:00'
        except Exception:
            high_watermark = '1900-01-01 00:00:00'
    else:
        high_watermark = '1900-01-01 00:00:00'
        
    print(f"High Watermark Detected: {high_watermark}")

    # 2. INCREMENTAL EXTRACTION 🚀
    df_incremental = spark.table(source_table).filter(col("bronze_ingestion_ts") > high_watermark)
    
    if df_incremental.isEmpty():
        print(f"🛑 No new data found for {table_name}. Skipping compute! 💸")
        continue

    # 3. CLEANSE & INJECT SCD2 COLUMNS 🧬
    df_scd = df_incremental \
        .withColumn(pk_column, col(pk_column).cast("integer")) \
        .dropna(subset=[pk_column]) \
        .dropDuplicates([pk_column]) \
        .withColumn("is_active", lit(True)) \
        .withColumn("valid_from", current_timestamp()) \
        .withColumn("valid_to", lit(None).cast("timestamp")) \
        .withColumn("silver_ingestion_ts", current_timestamp())

    # 4. THE EXECUTION 🧠
    if not table_exists:
        print(f"Executing Day-0 Initial Load for {table_name}... 🚀")
        df_scd.writeTo(target_table) \
            .option("path", physical_path) \
            .tableProperty("delta.autoOptimize.optimizeWrite", "true") \
            .tableProperty("delta.autoOptimize.autoCompact", "true") \
            .create()
    else:
        print(f"Executing Staff-Level SCD Type 2 MERGE for {table_name}... 🔄")
        target_delta = DeltaTable.forPath(spark, physical_path)
        
        # The Staged Updates Pattern for SCD2
        df_updates = df_scd.withColumn("merge_key", col(pk_column))
        df_inserts = df_scd.withColumn("merge_key", lit(None))
        staged_updates = df_updates.unionByName(df_inserts)
        
        target_delta.alias("target").merge(
            staged_updates.alias("source"),
            f"target.{pk_column} = source.merge_key AND target.is_active = True"
        ).whenMatchedUpdate(
            set = {
                "is_active": lit(False),
                "valid_to": current_timestamp()
            }
        ).whenNotMatchedInsertAll().execute()
        
        print("✅ Incremental SCD Type 2 Merge Complete! 🛡️")

print("\nAll Dimensions Processed! The Silver Layer is 100% Complete. 🏆")
