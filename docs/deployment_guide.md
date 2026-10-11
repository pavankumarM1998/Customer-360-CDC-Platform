# Customer 360 CDC Platform Deployment Guide (Legacy Pipeline)

This guide details deployment procedures using **Databricks Asset Bundles (DAB)** and pre-flight environment provisioning for the multi-notebook pipeline on the `Old` branch.

---

## 🛠️ Step 1: Pre-Flight Provisioning

Run `scripts/databricks_create_resources.sql` in the Databricks SQL Editor:
```sql
CREATE CATALOG IF NOT EXISTS customer360_dev;
CREATE VOLUME IF NOT EXISTS customer360_dev.staging.source_files;
```

Run `scripts/setup_folder_resources.py` to create the volume subdirectories:
```bash
python scripts/setup_folder_resources.py
```

---

## 🚀 Step 2: Databricks Asset Bundle (DAB) Deployment

Validate and deploy the bundle workflows:
```bash
# Validate bundle
databricks bundle validate -t dev

# Deploy to development
databricks bundle deploy -t dev
```

---

## 🏃 Step 3: Trigger Pipeline Executions

```bash
# Run Initial Master Pipeline
databricks bundle run customer360_initial_master_pipeline -t dev

# Run Incremental CDC Master Pipeline
databricks bundle run customer360_incremental_master_pipeline -t dev
```
