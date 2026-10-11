# Customer 360 CDC Platform

A production-grade, enterprise Lakehouse platform executing end-to-end Change Data Capture (CDC), Slowly Changing Dimensions (SCD Type 2), and Medallion Architecture processing for Customer 360 analytics, powered by **Databricks**, **Delta Lake**, **Unity Catalog**, and **Databricks Asset Bundles (DAB)**.

---

## 🏗️ Architecture & Component Overview

The platform implements a unified Medallion Lakehouse architecture across 8 core domain entities:

```
Customer-360-CDC-Platform/
│
├── notebooks/                              # Modular Databricks Notebooks (Bronze -> Silver -> Gold)
│   ├── 00_project_setup.py                 # Unity Catalog, schemas, volume, and control tables
│   ├── 01_staging_validation.py             # Landing files validation & quarantine routing
│   ├── 02_bronze_ingestion.py              # Parameterized Bronze ingestion for all 8 entities
│   ├── 03_bronze_reconciliation.py         # Staging to Bronze row-count reconciliation
│   ├── 04_silver_entity_processing.py      # Cleansing, typing, deduplication & quarantine
│   ├── 05_silver_reconciliation.py         # Bronze to Silver mathematical reconciliation
│   ├── 06_cdc_classification.py            # INSERT / UPDATE / UNCHANGED CDC classifier
│   ├── 07_cdc_merge.py                     # Delta Lake MERGE INTO processor
│   ├── 08_watermark_update.py              # Incremental watermark state manager
│   ├── 09_customer_scd2.py                 # Customer Slowly Changing Dimension Type 2
│   ├── 10_gold_build.py                    # Analytical dimensions, facts & agg_customer_360
│   ├── 11_reconciliation_archive.py        # End-to-end reconciliation & file archiving
│   ├── 12_failure_recovery.py              # Delta time travel restore & replay recovery
│   ├── 13_monitoring.py                    # Pipeline health metrics & operational SLA tracking
│   └── 14_optimization.py                  # Delta OPTIMIZE & file compaction maintenance
│
├── resources/                              # Databricks Asset Bundle (DAB) Workflow Definitions
│   ├── customer360_initial_pipeline.job.yml      # End-to-end Initial Baseline Load Pipeline
│   ├── customer360_incremental_pipeline.job.yml  # End-to-end Incremental CDC Pipeline
│   ├── customer360_maintenance.job.yml           # Optimization, compaction & monitoring pipeline
│   └── workspace_pinned/                         # Workspace-pinned reference jobs
│
├── scripts/                                # Provisioning & Utility Scripts
│   ├── databricks_create_resources.sql     # Unity Catalog DDL for catalog, schemas & volume
│   ├── setup_folder_resources.py           # Landing directory hierarchy initializer
│   └── generate_sample_data.py             # Synthetic sample CSV generator
│
├── sample_data/                            # Synthetic Testing Data for All 8 Entities
│   ├── initial/                            # Baseline CSVs (customers, orders, products, etc.)
│   ├── incremental/                        # CDC change CSVs (updates, inserts, status changes)
│   └── README.md                           # Sample data dictionary & usage guide
│
├── tests/                                  # Automated Test Suite
│   └── test_notebook_syntax.py             # Python syntax & compile validation tests
│
├── docs/                                   # Platform Documentation
│   ├── architecture.md                     # Medallion & CDC architectural diagrams
│   ├── data_dictionary.md                  # Comprehensive schema specifications
│   ├── deployment_guide.md                 # DAB & Databricks Repos deployment runbook
│   └── README.md
│
├── .github/workflows/                      # Continuous Integration (CI/CD)
│   └── databricks_bundle_ci.yml            # Bundle validation & Python syntax checks
│
├── databricks.yml                          # Databricks Asset Bundle Manifest (dev, staging, prod)
├── .gitignore                              # Production Git Ignore for Databricks Lakehouse
└── README.md                               # Project Root Documentation
```

> ℹ️ **Legacy Archive**: Historical legacy Databricks notebooks are preserved on the [`Old`](https://github.com/pavankumarM1998/Customer-360-CDC-Platform/tree/Old) branch.

---

## 🌟 Key Features

* **Medallion Lakehouse Processing**:
  * **Staging Area**: Unity Catalog Volumes (`/Volumes/customer360_dev/staging/source_files/`).
  * **Bronze Layer**: Raw append-only Delta tables with lineage metadata (`customer360_dev.bronze`).
  * **Silver Layer**: Cleaned, standardized, deduplicated domain entities (`customer360_dev.silver`).
  * **Gold Layer**: Business dimensions, SCD Type 2 history, and `agg_customer_360` view (`customer360_dev.gold`).
* **Automated Change Data Capture (CDC)**:
  * Delta Lake `MERGE INTO` operations driven by automated watermark state tracking (`control.watermark_control`).
  * `INSERT`, `UPDATE`, and `UNCHANGED` classification preventing stale updates.
* **Customer SCD Type 2**:
  * Tracks historical customer changes (`effective_from`, `effective_to`, `is_current`) with zero technical column leakage.
* **Resilience & Governance**:
  * Cross-layer mathematical reconciliation at Bronze and Silver tiers.
  * Delta time travel rollback and replay recovery routines.

---

## 🚀 Quick Start

### 1. Provision Unity Catalog Infrastructure
Run `scripts/databricks_create_resources.sql` in the Databricks SQL Editor to create catalog `customer360_dev`, schemas, and staging volume:
```sql
CREATE CATALOG IF NOT EXISTS customer360_dev;
CREATE VOLUME IF NOT EXISTS customer360_dev.staging.source_files;
```

### 2. Initialize Volume Landing Directories
Run `scripts/setup_folder_resources.py` or execute `notebooks/00_project_setup.py`:
```bash
python scripts/setup_folder_resources.py
```

### 3. Deploy Databricks Asset Bundle (DAB)
Deploy the pipelines to development using the Databricks CLI:
```bash
# Validate bundle
databricks bundle validate -t dev

# Deploy workflows and notebooks
databricks bundle deploy -t dev
```

### 4. Trigger Pipeline Executions
```bash
# Run Initial Baseline Pipeline
databricks bundle run customer360_initial_pipeline -t dev

# Run Incremental CDC Pipeline
databricks bundle run customer360_incremental_pipeline -t dev
```

---

## 🧪 Testing & Validation

Run local unit tests and syntax checks:
```bash
python -m unittest tests/test_notebook_syntax.py
```
