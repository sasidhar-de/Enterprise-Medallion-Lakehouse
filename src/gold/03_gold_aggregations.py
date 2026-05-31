# Databricks notebook source
# ==============================================================================
# SCRIPT: 03_gold_aggregations.py
# LAYER: GOLD (MANAGED)
# DESCRIPTION: Heavy business aggregations for QxImpact BI Dashboards.
# ==============================================================================

from pyspark.sql.functions import col, sum, max, current_date, date_sub, lit, when

print("Igniting the Gold Forge... 👑📈")

spark.conf.set("spark.sql.shuffle.partitions", "1024")

# Enable Adaptive Query Execution (AQE) 
# If 1024 is too many, Databricks will dynamically merge the tiny ones!
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# 1. Read from the Silver Clean Room
df_sales = spark.table("tpcds_enterprise.silver.store_sales")
df_items = spark.table("tpcds_enterprise.silver.item")
df_customers = spark.table("tpcds_enterprise.silver.customer")
df_dates = spark.table("tpcds_enterprise.silver.date_dim")

# ==============================================================================
# 🥇 GOLD TABLE 1: Executive Monthly Sales Summary
# ==============================================================================
print("Calculating Executive Monthly Sales...")

df_monthly_sales = df_sales.join(df_items, df_sales.ss_item_sk == df_items.i_item_sk) \
    .join(df_dates, df_sales.ss_sold_date_sk == df_dates.d_date_sk) \
    .groupBy("d_year", "d_moy", "i_category", "i_item_id") \
    .agg(
        sum("ss_quantity").alias("total_units_sold"),
        sum("ss_net_paid").alias("total_net_revenue")
    )

# Write as a MANAGED table (Notice there is NO .option("path") here!)
df_monthly_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("tpcds_enterprise.gold.executive_monthly_sales")

# COMMAND ----------

from pyspark.sql.functions import datediff

# ==============================================================================
# 🥇 GOLD TABLE 2: The 120-Day Customer Churn Risk
# ==============================================================================
print("Analyzing Customer Retention & 120-Day Churn Risk...")

# First, find the absolute latest order date for every customer who HAS ordered.
df_latest_orders = df_sales.join(df_dates, df_sales.ss_sold_date_sk == df_dates.d_date_sk) \
    .groupBy("ss_customer_sk") \
    .agg(max("d_date").alias("last_order_date"))

# The Staff DE Masterstroke: Left join all customers to find the ghosts.
# We must catch both logic gates: 
# 1. Customers who ordered, but not in the last 120 days.
# 2. Customers who registered but NEVER ordered (last_order_date is NULL).
df_churn_risk = df_customers.join(df_latest_orders, df_customers.c_customer_sk == df_latest_orders.ss_customer_sk, "left") \
    .withColumn("days_since_last_order", 
                when(col("last_order_date").isNotNull(), datediff(current_date(), col("last_order_date")))
                .otherwise(lit(9999))) \
    .filter(
        col("last_order_date").isNull() | 
        (col("last_order_date") < date_sub(current_date(), 120))
    ) \
    .select("c_customer_sk", "c_first_name", "c_last_name", "last_order_date", "days_since_last_order")

# Write as a MANAGED table for instant BI querying
df_churn_risk.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("tpcds_enterprise.gold.customer_churn_120_days")

print("✅ Gold Layer Execution Complete! Dashboards are ready")
