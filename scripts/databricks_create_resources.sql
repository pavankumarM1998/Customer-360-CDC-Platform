-- =====================================================================
-- Databricks Unity Catalog & Volume Provisioning Script
-- Platform: Customer 360 CDC Platform
-- =====================================================================

-- 1. Create Catalog
CREATE CATALOG IF NOT EXISTS customer360_dev;
USE CATALOG customer360_dev;

-- 2. Create Medallion & Governance Schemas
CREATE SCHEMA IF NOT EXISTS customer360_dev.staging;
CREATE SCHEMA IF NOT EXISTS customer360_dev.bronze;
CREATE SCHEMA IF NOT EXISTS customer360_dev.silver;
CREATE SCHEMA IF NOT EXISTS customer360_dev.gold;
CREATE SCHEMA IF NOT EXISTS customer360_dev.control;

-- 3. Create Unity Catalog Staging Volume
CREATE VOLUME IF NOT EXISTS customer360_dev.staging.source_files;

-- Note: Source subdirectories for initial, incremental, and archive ingestion 
-- under /Volumes/customer360_dev/staging/source_files/ are provisioned by 
-- running scripts/setup_folder_resources.py or notebook 01_create_staging_structure.ipynb.
