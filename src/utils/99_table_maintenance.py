# Databricks notebook source
# 6. Z-Order Optimization for Downstream Speed
spark.sql(f"""
    OPTIMIZE {TARGET_TABLE}
    ZORDER BY (ss_item_sk, ss_customer_sk)
""")