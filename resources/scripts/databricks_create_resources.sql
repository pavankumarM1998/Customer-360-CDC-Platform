-- =====================================================================
-- Databricks Unity Catalog & Volume Resource Setup Script
-- Platform: Customer 360 CDC Platform
-- =====================================================================

-- 1. Create Catalog
CREATE CATALOG IF NOT EXISTS customer360_dev;
USE CATALOG customer360_dev;

-- 2. Create Medallion Schemas
CREATE SCHEMA IF NOT EXISTS customer360_dev.staging;
CREATE SCHEMA IF NOT EXISTS customer360_dev.bronze;
CREATE SCHEMA IF NOT EXISTS customer360_dev.silver;
CREATE SCHEMA IF NOT EXISTS customer360_dev.gold;
CREATE SCHEMA IF NOT EXISTS customer360_dev.control;

-- 3. Create Unity Catalog Staging Volume
CREATE VOLUME IF NOT EXISTS customer360_dev.staging.source_files;

-- Volume subdirectories created dynamically by Notebook 01:
-- /Volumes/customer360_dev/staging/source_files/customers/{initial, incremental, archive}
-- /Volumes/customer360_dev/staging/source_files/addresses/{initial, incremental, archive}
-- /Volumes/customer360_dev/staging/source_files/subscriptions/{initial, incremental, archive}
-- /Volumes/customer360_dev/staging/source_files/products/{initial, incremental, archive}
-- /Volumes/customer360_dev/staging/source_files/orders/{initial, incremental, archive}
-- /Volumes/customer360_dev/staging/source_files/order_items/{initial, incremental, archive}
-- /Volumes/customer360_dev/staging/source_files/payments/{initial, incremental, archive}
-- /Volumes/customer360_dev/staging/source_files/support_tickets/{initial, incremental, archive}
