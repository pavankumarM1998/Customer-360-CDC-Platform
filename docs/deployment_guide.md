# Customer 360 CDC Platform Deployment Guide

This guide details deployment procedures using **Databricks Asset Bundles (DAB)**, **Databricks Repos / Git Folders**, and pre-flight environment provisioning.

---

## 📋 Prerequisites

1. **Databricks CLI**: Version `0.220.0` or later installed and authenticated:
   ```bash
   databricks auth login --host https://<databricks-workspace-url>
   ```
2. **Unity Catalog Privileges**:
   - `CREATE CATALOG` or admin ownership of `customer360_dev`
   - Permissions to create volumes and schemas

---

## 🛠️ Step 1: Pre-Flight Environment Provisioning

### 1. Execute SQL DDL
Run the resource provisioning script in the Databricks SQL Editor or as a setup notebook:
```sql
-- Execute scripts/databricks_create_resources.sql
CREATE CATALOG IF NOT EXISTS customer360_dev;
USE CATALOG customer360_dev;

CREATE SCHEMA IF NOT EXISTS customer360_dev.staging;
CREATE SCHEMA IF NOT EXISTS customer360_dev.bronze;
CREATE SCHEMA IF NOT EXISTS customer360_dev.silver;
CREATE SCHEMA IF NOT EXISTS customer360_dev.gold;
CREATE SCHEMA IF NOT EXISTS customer360_dev.control;
CREATE SCHEMA IF NOT EXISTS customer360_dev.quarantine;

CREATE VOLUME IF NOT EXISTS customer360_dev.staging.source_files;
```

### 2. Initialize Volume Landing Hierarchy
Execute `scripts/setup_folder_resources.py` within Databricks (or run notebook `notebooks/00_project_setup.py`) to create the subdirectories:
```
/Volumes/customer360_dev/staging/source_files/{entity}/
├── initial/
├── incremental/
├── rejected/
└── archive/
```

---

## 🚀 Step 2: Databricks Asset Bundle (DAB) Deployment

Databricks Asset Bundles allow you to validate, deploy, and execute the entire project programmatically across environments:

### 1. Validate the Bundle
Verify that all resource YAMLs and relative notebook paths are structurally valid:
```bash
databricks bundle validate -t dev
```

### 2. Deploy to Development
Deploy workflows and bundle assets to your development workspace:
```bash
databricks bundle deploy -t dev
```

### 3. Deploy to Production
Deploy workflows to the production target:
```bash
databricks bundle deploy -t prod
```

---

## 🏃 Step 3: Pipeline Execution Runbook

### Cold Start: Initial Baseline Load
Trigger the initial end-to-end pipeline to validate files, populate Bronze, establish Silver baselines, and initialize Customer SCD Type 2 history:
```bash
databricks bundle run customer360_initial_pipeline -t dev
```

### Continuous Operation: Incremental CDC Load
Trigger the incremental CDC pipeline to classify changes, execute Delta MERGE, update watermarks, version SCD2 changes, and archive processed files:
```bash
databricks bundle run customer360_incremental_pipeline -t dev
```

### Maintenance: Table Optimization
Run Delta optimizations, vacuuming, and health checks:
```bash
databricks bundle run customer360_maintenance_pipeline -t dev
```
