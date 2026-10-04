# Customer 360 CDC Platform Architecture Documentation

## Overview
The **Customer 360 CDC Platform** is an enterprise data engineering platform built on **Databricks Lakehouse**, leveraging **Delta Lake**, **Unity Catalog**, and **Databricks Asset Bundles (DAB)**.

## Key Capabilities

### 1. Medallion Lakehouse Architecture
- **Staging / Unity Catalog Volumes:** Landing zone (`/Volumes/customer360_dev/staging/source_files/`) for initial baselines and incremental CDC source files across 8 domain entities (customers, addresses, subscriptions, products, orders, order_items, payments, support_tickets).
- **Bronze Layer (`customer360_dev.bronze`):** Raw append-only Delta tables with ingested schema validation, raw record tracking, and source file metadata.
- **Silver Layer (`customer360_dev.silver`):** Cleaned, standardized, deduplicated, and validated data tables with quality enforcement and reconciliation metrics.
- **Gold Layer (`customer360_dev.gold`):** Business-ready analytical models including `dim_product`, `fact_orders`, `fact_order_items`, `fact_payments`, `fact_support_tickets`, and unified `agg_customer_360` view.

### 2. Change Data Capture (CDC) & SCD Type 2
- **Incremental CDC Processing:** Automated change classification (`INSERT`, `UPDATE`, `DELETE`) with watermark tracking and reconciliation controls.
- **Slowly Changing Dimensions (SCD Type 2):** Complete historical tracking (`is_current`, `effective_start_date`, `effective_end_date`) for customer dimensional attributes (`customer_scd2`).

### 3. Governance & Control Framework
- **Unity Catalog:** Centralized governance, access control, volume management, and catalog scoping (`customer360_dev`).
- **Audit & Watermarking:** Managed audit logging and incremental watermark state tracking under `customer360_dev.control`.
- **Recovery & Restore Framework:** Built-in table version restoration and disaster recovery routines.

## Orchestration & Deployment Layout
- `resources/`: Databricks Asset Bundle (DAB) workflow and job definitions (`*.job.yml`).
- `scripts/`: Infrastructure resource initialization SQL and Unity Catalog volume setup scripts.
- `Customer 360 & CDC Platform/`: Databricks Notebooks containing all end-to-end data transformation logic.
