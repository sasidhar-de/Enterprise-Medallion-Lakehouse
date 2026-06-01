# Databricks notebook source
# Databricks notebook source
# ==============================================================================
# SCRIPT: 03_gold_aggregations.py
# LAYER: GOLD (BUSINESS VALUE)
# DESCRIPTION: Petabyte-Scale Incremental Aggregations & State Compute
# ==============================================================================

from pyspark.sql.functions import col
from delta.tables import DeltaTable

print("Unlocking the Gold Executive Vault... 🏆📈")

SILVER_DB = "tpcds_enterprise.silver"
GOLD_DB = "tpcds_enterprise.gold"
ADLS_GOLD_PATH = "abfss://tpc-ds@stpraxaslakehouse.dfs.core.windows.net/data/gold_zone"


spark.conf.set("spark.sql.shuffle.partitions", "1024")

# Enable Adaptive Query Execution (AQE) 
# If 1024 is too many, Databricks will dynamically merge the tiny ones!
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

from pyspark.sql.functions import col
from delta.tables import DeltaTable

print("Unlocking the Gold Executive Vault... 🏆📈")

SILVER_DB = "tpcds_enterprise.silver"
GOLD_DB = "tpcds_enterprise.gold"
ADLS_GOLD_PATH = "abfss://tpc-ds@stpraxaslakehouse.dfs.core.windows.net/data/gold_zone"

# ==============================================================================
# MART 1: EXECUTIVE MONTHLY SALES (Incremental MERGE Architecture) 🚀
# ==============================================================================
print("Checking for existing High Watermark in Gold Sales...")
gold_sales_path = f"{ADLS_GOLD_PATH}/executive_monthly_sales"
sales_table_exists = DeltaTable.isDeltaTable(spark, gold_sales_path)

if sales_table_exists:
    try:
        max_ts_df = spark.sql(f"SELECT MAX(gold_update_ts) FROM {GOLD_DB}.executive_monthly_sales")
        high_watermark = max_ts_df.collect()[0][0]
        if high_watermark is None:
            high_watermark = '1900-01-01 00:00:00'
    except Exception:
        high_watermark = '1900-01-01 00:00:00'
else:
    high_watermark = '1900-01-01 00:00:00'

print(f"Executing Micro-Batch Aggregation for data after {high_watermark}...")

# 1. We ONLY aggregate the new silver data!
delta_sales_df = spark.sql(f"""
    SELECT 
        d.d_year AS sales_year,
        d.d_moy AS sales_month,
        i.i_category AS product_category,
        SUM(s.ss_quantity) AS incremental_quantity,
        SUM(s.ss_net_paid) AS incremental_revenue,
        current_timestamp() AS gold_update_ts
    FROM {SILVER_DB}.store_sales s
    
    JOIN {SILVER_DB}.date_dim d ON s.ss_sold_date_sk = d.d_date_sk AND d.is_active = True
    JOIN {SILVER_DB}.item i ON s.ss_item_sk = i.i_item_sk AND i.is_active = True
    
    -- The Petabyte-Scale Secret: Only process fresh receipts!
    WHERE s.silver_ingestion_ts > '{high_watermark}'
        
    GROUP BY d.d_year, d.d_moy, i.i_category
""")

if not delta_sales_df.isEmpty():
    if not sales_table_exists:
        print("Executing Day-0 Initial Gold Sales Load...")
        delta_sales_df.withColumnRenamed("incremental_quantity", "total_quantity_sold") \
                      .withColumnRenamed("incremental_revenue", "total_revenue") \
                      .writeTo(f"{GOLD_DB}.executive_monthly_sales") \
                      .option("path", gold_sales_path) \
                      .partitionedBy("sales_year", "sales_month") \
                      .create()
    else:
        print("Executing Staff-Level Incremental MERGE for Gold Sales... 🔄")
        gold_sales_table = DeltaTable.forPath(spark, gold_sales_path)
        
        # We mathematically ADD the new batch totals to the existing historical totals!
        gold_sales_table.alias("target").merge(
            delta_sales_df.alias("source"),
            "target.sales_year = source.sales_year AND target.sales_month = source.sales_month AND target.product_category = source.product_category"
        ).whenMatchedUpdate(
            set = {
                "total_quantity_sold": "target.total_quantity_sold + source.incremental_quantity",
                "total_revenue": "target.total_revenue + source.incremental_revenue",
                "gold_update_ts": "source.gold_update_ts"
            }
        ).whenNotMatchedInsert(
            values = {
                "sales_year": "source.sales_year",
                "sales_month": "source.sales_month",
                "product_category": "source.product_category",
                "total_quantity_sold": "source.incremental_quantity",
                "total_revenue": "source.incremental_revenue",
                "gold_update_ts": "source.gold_update_ts"
            }
        ).execute()
        print("✅ Incremental Gold Sales MERGE Complete!")
else:
    print("🛑 No new sales data found. Skipping Gold Sales compute! 💸")




# COMMAND ----------

# ==============================================================================
# MART 2: CUSTOMER CHURN (120 DAYS)
# ==============================================================================
print("Executing Staff-Level logic for 120-Day Customer Churn...")

# The mathematical beauty here is the LEFT JOIN. 
# We explicitly capture customers whose last order was > 120 days ago, 
# AND customers who registered but mathematically NEVER ordered (last_order_date IS NULL).
customer_churn_df = spark.sql(f"""
    WITH LastCustomerPurchase AS (
        SELECT 
            ss_customer_sk, 
            MAX(d.d_date) AS last_order_date
        FROM {SILVER_DB}.store_sales ss
        JOIN {SILVER_DB}.date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
        GROUP BY ss_customer_sk
    )
    
    SELECT 
        c.c_customer_sk AS customer_id,
        c.c_first_name AS first_name,
        c.c_last_name AS last_name,
        c.c_email_address AS email,
        p.last_order_date,
        CASE 
            WHEN p.last_order_date IS NULL THEN 'Never Ordered'
            ELSE 'Churned (>120 Days)'
        END AS churn_status
    FROM {SILVER_DB}.customer c
    
    -- LEFT JOIN is critical to find the 'Never Ordered' cohort
    LEFT JOIN LastCustomerPurchase p ON c.c_customer_sk = p.ss_customer_sk
    
    WHERE c.is_active = True
      AND (
          p.last_order_date IS NULL 
          OR datediff(current_date(), p.last_order_date) >= 120
      )
""")

print("Publishing customer_churn_120_days...")
customer_churn_df.writeTo(f"{GOLD_DB}.customer_churn_120_days") \
    .option("path", f"{ADLS_GOLD_PATH}/customer_churn_120_days") \
    .partitionedBy("churn_status") \
    .tableProperty("delta.autoOptimize.optimizeWrite", "true") \
    .tableProperty("delta.autoOptimize.autoCompact", "true") \
    .createOrReplace()

print("✅ Gold Layer Published! The enterprise dashboards are mathematically pure. 💰")
