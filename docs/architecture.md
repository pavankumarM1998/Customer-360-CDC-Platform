# Customer 360 CDC Platform Architecture (Legacy Pipeline)

This document describes the architectural flow, component designs, data flow patterns, and governance models for the **Customer 360 CDC Platform** (Historical Multi-Notebook Pipeline on `Old` branch).

---

## 🏗️ 1. Medallion Lakehouse Flow

The platform implements an end-to-end Medallion Lakehouse pipeline powered by **Databricks**, **Delta Lake**, and **Unity Catalog**:

1. **Staging / Landing Zone**:
   - Unity Catalog Volume: `/Volumes/customer360_dev/staging/source_files/{entity}/`
   - Subdirectories: `initial/`, `incremental/`, `archive/`, `rejected/`.

2. **Bronze Layer (`customer360_dev.bronze`)**:
   - Notebooks `04_bronze_customers_initial_load.ipynb`, `05_bronze_initial_load_all.ipynb`, `24_bronze_customers_incremental.ipynb`, etc.
   - Preserves raw ingested attributes with technical lineage metadata.
   - Cross-reconciliation executed via `06_bronze_reconciliation.ipynb`.

3. **Silver Layer (`customer360_dev.silver`)**:
   - Entity-specific cleansing, standardization, deduplication, and domain rule enforcement (Notebooks `07` through `22`).
   - Incremental Change Data Capture (CDC) classification (`INSERT`, `UPDATE`, `UNCHANGED`) via notebooks `25_classify_customer_cdc.ipynb`, `30_classify_address_cdc.ipynb`, etc.
   - Delta Lake `MERGE INTO` execution via `31_merge_address_cdc.ipynb`, `36__merge_subscription_cdc.ipynb`, etc.
   - Watermark advancement via `27_update_customer_watermark.ipynb`, `33_update_address_watermark.ipynb`, etc.

4. **Gold Layer (`customer360_dev.gold`)**:
   - Customer SCD Type 2 dimension versioning (`40` through `45_reconcile_customer_scd2.ipynb`).
   - Dimensional and fact models (`dim_product`, `agg_customer_360`).

5. **Governance & Control (`customer360_dev.control`)**:
   - Comprehensive audit control framework (`49_audit_control_framework.ipynb`).
   - Disaster recovery and Delta time-travel restoration routines (`50_recovery_restore_framework.ipynb`).
