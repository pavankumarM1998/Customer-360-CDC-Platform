# Customer 360 CDC Platform

A production-grade Databricks Lakehouse platform executing end-to-end Change Data Capture (CDC), Slowly Changing Dimensions (SCD Type 2), and Medallion Architecture processing for Customer 360 analytics.

---

## 🏗️ Architecture & Component Overview

This project is built on **Databricks**, **Delta Lake**, **Unity Catalog**, and **Databricks Asset Bundles (DAB)**.

```
Customer-360-CDC-Platform/
│
├── Customer 360 CDC Platform/       # End-to-end Python CDC & Data Pipeline Scripts
│
├── resources/                        # Databricks Asset Bundle (DAB) Workflow / Job YAMLs
│   ├── customer360_initial_pipeline.job.yml      # End-to-end Initial Load Pipeline
│   └── customer360_incremental_pipeline.job.yml  # End-to-end CDC Incremental Pipeline
│
├── scripts/                          # Environment & Resource Setup Scripts
│   ├── setup_folder_resources.py     # Unity Catalog Volume folder structure initializer
│   └── databricks_create_resources.sql # Catalog, Schema, and Volume DDL script
│
├── docs/                             # Platform Architecture & Design Documentation
│   └── README.md
│
├── sample_data/                      # Small sample/testing data files (optional)
│
├── databricks.yml                    # Databricks Asset Bundle Manifest
└── README.md                         # Project Root Readme
```

> ℹ️ **Legacy Archive**: Historical legacy Databricks notebooks and configurations are archived on the [`Old`](https://github.com/pavankumarM1998/Customer-360-CDC-Platform/tree/Old) branch.

---

## 🌟 Key Features

* **Medallion Lakehouse Architecture**:
  * **Staging Area**: Unity Catalog Volumes (`/Volumes/customer360_dev/staging/source_files/`).
  * **Bronze Layer**: Raw ingested records and initial baselines (`customer360_dev.bronze`).
  * **Silver Layer**: Cleaned, standardized, deduplicated domain entities (`customer360_dev.silver`).
  * **Gold Layer**: Business dimensions, SCD Type 2 history, and `agg_customer_360` view (`customer360_dev.gold`).
* **Change Data Capture (CDC) & SCD Type 2**:
  * Incremental change detection (`INSERT`, `UPDATE`, `DELETE`) with automated watermarking and reconciliation.
  * Historical versioning (`effective_start_date`, `effective_end_date`, `is_current`) for customer dimensions.
* **Governance & Data Storage**:
  * **Unity Catalog**: Managed catalog (`customer360_dev`) and schemas (`staging`, `bronze`, `silver`, `gold`, `control`).
  * **Data Storage Note**: Actual data and landing files live in **Databricks Unity Catalog Volumes** and **Delta Lake Tables**, not in source control.

---

## 🚀 Quick Start

### 1. Provision Unity Catalog Resources
Run the environment setup SQL script in Databricks SQL Editor or Notebook:
```bash
scripts/databricks_create_resources.sql
```

### 2. Initialize Volume Landing Subdirectories
Execute the volume folder setup script inside Databricks:
```bash
python scripts/setup_folder_resources.py
```

### 3. Deploy Databricks Asset Bundles (DAB)
Deploy the workflows to your target environment using the Databricks CLI:
```bash
databricks bundle deploy -t dev
```
