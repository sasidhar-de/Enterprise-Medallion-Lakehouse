-- Databricks notebook source
-- ==============================================================================
-- SCRIPT: 00_unity_catalog_setup.sql
-- DESCRIPTION: Setting up the Enterprise Governance layer with explicit storage
-- ==============================================================================

-- 1. Create the Master Enterprise Catalog WITH a Managed Location
CREATE CATALOG IF NOT EXISTS tpcds_enterprise
  MANAGED LOCATION 'abfss://tpc-ds@stpraxaslakehouse.dfs.core.windows.net/tpcds_metadata/';

-- 2. Create the Medallion Schemas (Databases)[cite: 1]
-- These point directly to your dedicated layer folders[cite: 1]
CREATE SCHEMA IF NOT EXISTS tpcds_enterprise.bronze 
  MANAGED LOCATION 'abfss://tpc-ds@stpraxaslakehouse.dfs.core.windows.net/data/bronze_zone/';

CREATE SCHEMA IF NOT EXISTS tpcds_enterprise.silver 
  MANAGED LOCATION 'abfss://tpc-ds@stpraxaslakehouse.dfs.core.windows.net/data/silver_zone/';

CREATE SCHEMA IF NOT EXISTS tpcds_enterprise.gold 
  MANAGED LOCATION 'abfss://tpc-ds@stpraxaslakehouse.dfs.core.windows.net/data/gold_zone/';

