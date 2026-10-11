# Customer 360 CDC Platform (Legacy Multi-Notebook Pipeline)

> ℹ️ **Branch Information**: This branch (`Old`) contains the historical multi-notebook pipeline (50+ modular `.ipynb` notebooks). For the consolidated 15-pipeline modular architecture, see the [`main`](https://github.com/pavankumarM1998/Customer-360-CDC-Platform/tree/main) branch.

A production-grade Databricks Lakehouse platform executing end-to-end Change Data Capture (CDC), Slowly Changing Dimensions (SCD Type 2), and Medallion Architecture processing for Customer 360 analytics, powered by **Databricks**, **Delta Lake**, **Unity Catalog**, and **Databricks Asset Bundles (DAB)**.

---

## 🏗️ Architecture & Component Overview

```
Customer-360-CDC-Platform/ (Old Branch)
│
├── notebooks/                              # Modular Historical Databricks Notebooks (50+ .ipynb files)
│   ├── 01_create_staging_structure.ipynb   # Volume folder setup
│   ├── 02_Validate Customer Source Delivery.ipynb # Customer landing validation
│   ├── 03_validate_all_sources.ipynb       # Validation across all 8 entities
│   ├── 04_bronze_customers_initial_load.ipynb # Bronze customer initial ingestion
│   ├── 05_bronze_initial_load_all.ipynb    # Bronze initial load for all entities
│   ├── 06_bronze_reconciliation.ipynb      # Bronze reconciliation
│   ├── 07_silver_customers.ipynb           # Silver customer cleansing
│   ├── ...                                 # Silver entity processing (08 - 22)
│   ├── 23_create_cdc_control.ipynb         # CDC control tables DDL
│   ├── 24_bronze_customers_incremental.ipynb # Incremental Bronze loads
│   ├── 25_classify_customer_cdc.ipynb      # CDC classification engine
│   ├── ...                                 # CDC processing per entity (26 - 39)
│   ├── 40_create_customer_scd2.ipynb       # SCD Type 2 dimension initialization
│   ├── ...                                 # SCD2 versioning and expiration (41 - 45)
│   ├── 46_build_dim_product.ipynb          # Dimensional product modeling
│   ├── 48_build_gold_customer360.ipynb     # Unified Customer 360 aggregation
│   ├── 49_archive_processed_source_files.ipynb # Source file archiving
│   ├── 49_audit_control_framework.ipynb    # Control and audit framework
│   └── 50_recovery_restore_framework.ipynb # Disaster recovery and replay
│
├── resources/                              # Databricks Asset Bundle (DAB) Workflow Definitions
│   ├── customer_initial_master.job.yml     # End-to-end Initial Load Master Workflow
│   ├── customer_incremental_master.job.yml # End-to-end Incremental CDC Master Workflow
│   ├── customer_staging.job.yml            # Staging validation workflow
│   ├── customer_bronze.job.yml             # Bronze layer ingestion workflow
│   ├── customer_silver.job.yml             # Silver layer conformed workflow
│   └── customer_gold.job.yml               # Gold layer analytics workflow
│
├── scripts/                                # Provisioning & Utility Scripts
│   ├── databricks_create_resources.sql     # Unity Catalog DDL for catalog, schemas & volume
│   ├── setup_folder_resources.py           # Landing directory hierarchy initializer
│   └── generate_sample_data.py             # Synthetic sample CSV generator
│
├── sample_data/                            # Synthetic Testing Data for All 8 Entities
│   ├── initial/                            # Baseline CSVs (customers, orders, products, etc.)
│   ├── incremental/                        # CDC change CSVs (updates, inserts, status changes)
│   └── README.md                           # Sample data schema guide & loading instructions
│
├── tests/                                  # Automated Test Suite
│   └── test_project_structure.py           # Test suite verifying notebook & script integrity
│
├── docs/                                   # Platform Documentation
│   ├── architecture.md                     # Medallion & CDC architectural flowcharts & design
│   ├── data_dictionary.md                  # Comprehensive schema specifications across Bronze, Silver, Gold
│   ├── deployment_guide.md                 # DAB deployment runbook
│   └── README.md
│
├── .github/workflows/                      # Continuous Integration (CI/CD)
│   └── databricks_bundle_ci.yml            # Bundle validation & Python syntax checks
│
├── databricks.yml                          # Databricks Asset Bundle Manifest (dev, staging, prod)
├── .gitignore                              # Production Git Ignore for Databricks Lakehouse
└── README.md                               # Project Root Documentation
```

---

## 🚀 Quick Start

### 1. Provision Unity Catalog Infrastructure
Run `scripts/databricks_create_resources.sql` in the Databricks SQL Editor:
```sql
CREATE CATALOG IF NOT EXISTS customer360_dev;
CREATE VOLUME IF NOT EXISTS customer360_dev.staging.source_files;
```

### 2. Initialize Volume Landing Directories
Run `scripts/setup_folder_resources.py`:
```bash
python scripts/setup_folder_resources.py
```

### 3. Deploy Databricks Asset Bundle (DAB)
```bash
# Validate bundle
databricks bundle validate -t dev

# Deploy workflows and notebooks
databricks bundle deploy -t dev
```

### 4. Trigger Pipeline Executions
```bash
# Initial Load
databricks bundle run customer360_initial_master_pipeline -t dev

# Incremental CDC Load
databricks bundle run customer360_incremental_master_pipeline -t dev
```
