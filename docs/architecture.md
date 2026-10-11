# Customer 360 CDC Platform Architecture

This document describes the architectural principles, component designs, data flow patterns, and governance models for the **Customer 360 CDC Platform**.

---

## 🏗️ 1. End-to-End Medallion Lakehouse Flow

The platform implements a multi-tier Medallion Architecture powered by **Databricks**, **Delta Lake**, and **Unity Catalog**:

```mermaid
flowchart TD
    subgraph Staging["1. Staging Landing Zone (Unity Catalog Volumes)"]
        LandingFiles["/Volumes/customer360_dev/staging/source_files/{entity}/<br/>• initial/<br/>• incremental/<br/>• rejected/<br/>• archive/"]
    end

    subgraph Validation["2. Staging Validation & Quarantine"]
        ValStep["01_staging_validation.py<br/>• CSV header checks<br/>• Business key presence<br/>• Row count threshold validation"]
        QuarTable["customer360_dev.quarantine.records"]
    end

    subgraph Bronze["3. Raw Bronze Layer"]
        BronzeIngest["02_bronze_ingestion.py<br/>• Raw string ingestion<br/>• Lineage metadata tagging (_source_file, _pipeline_run_id)"]
        BronzeTables["customer360_dev.bronze.{entity}<br/>(8 domain tables)"]
        BronzeRecon["03_bronze_reconciliation.py"]
    end

    subgraph Silver["4. Conformed Silver Layer"]
        CDCClass["06_cdc_classification.py<br/>• INSERT / UPDATE / UNCHANGED tagging"]
        CDCMerge["07_cdc_merge.py<br/>• Delta Lake MERGE INTO"]
        SilverProcess["04_silver_entity_processing.py<br/>• Cleansing, typing, deduplication"]
        SilverTables["customer360_dev.silver.{entity}"]
        Watermark["08_watermark_update.py<br/>• customer360_dev.control.watermark_control"]
        SilverRecon["05_silver_reconciliation.py"]
    end

    subgraph Gold["5. Analytical Gold Layer"]
        SCD2["09_customer_scd2.py<br/>• SCD Type 2 dimension history"]
        DimCust["customer360_dev.gold.dim_customer"]
        GoldBuild["10_gold_build.py<br/>• Business dimensional & fact models"]
        DimTables["dim_address, dim_product"]
        FactTables["fact_orders, fact_order_items, fact_payments,<br/>fact_subscriptions, fact_support_tickets"]
        Customer360["agg_customer_360 View"]
    end

    subgraph Control["6. Control, Governance & Archive"]
        Archive["11_reconciliation_archive.py<br/>• Move processed files to archive/"]
        AuditTables["customer360_dev.control.pipeline_runs<br/>customer360_dev.control.file_processing_status<br/>customer360_dev.control.reconciliation_results"]
    end

    LandingFiles --> ValStep
    ValStep -- Valid --> BronzeIngest
    ValStep -- Corrupt --> QuarTable
    BronzeIngest --> BronzeTables
    BronzeTables --> BronzeRecon
    BronzeRecon --> CDCClass
    CDCClass --> CDCMerge
    CDCMerge --> SilverProcess
    SilverProcess --> SilverTables
    SilverTables --> SilverRecon
    SilverTables --> Watermark
    SilverTables --> SCD2
    SilverTables --> GoldBuild
    SCD2 --> DimCust
    GoldBuild --> DimTables
    GoldBuild --> FactTables
    DimCust & DimTables & FactTables --> Customer360
    Customer360 --> Archive
    Archive --> AuditTables
```

---

## 🔄 2. Change Data Capture (CDC) Mechanics

The platform utilizes a state-aware CDC engine operating between Bronze and Silver layers:

1. **Watermark Scoping**:
   - Queries `customer360_dev.control.watermark_control` to fetch the highest previously processed watermark timestamp for the entity.
   - Restricts evaluation to newly arrived rows matching `PIPELINE_RUN_ID` or timestamp greater than the high watermark.

2. **Classification Logic**:
   - Incoming records are compared against the target Silver table by primary business key:
     - **`INSERT`**: Business key does not exist in Silver.
     - **`UPDATE`**: Business key exists in Silver, but one or more business attributes have changed and incoming `updated_at >= silver.updated_at`.
     - **`UNCHANGED`**: Incoming record is identical to the current Silver state. Stale or duplicate records are suppressed.

3. **Atomic Merge & Watermark Commit**:
   - Updates and inserts are merged using Delta Lake `MERGE INTO` with deterministic matching predicates.
   - Upon successful transaction commit, `08_watermark_update.py` advances the watermark in `customer360_dev.control.watermark_control`.

---

## 📜 3. Customer SCD Type 2 Architecture

Customer dimensions undergo historical versioning to maintain point-in-time analytical validity:

* **Surrogate Key**: SHA-256 hash or deterministic UUID based on `customer_id` and `effective_from`.
* **Attributes Tracked**: Status, tier, email, phone, and residency modifications.
* **Version Expiration**:
  - When an attribute change is detected on `customer_id`, the active record is expired:
    ```sql
    UPDATE customer360_dev.gold.dim_customer
    SET is_current = false, effective_to = CURRENT_TIMESTAMP()
    WHERE customer_id = :id AND is_current = true
    ```
  - A new record is inserted with:
    - `is_current = true`
    - `effective_from = CURRENT_TIMESTAMP()`
    - `effective_to = NULL`
* **Zero Leakage**: All technical ingestion columns (`_source_file`, `_pipeline_run_id`, etc.) are stripped prior to Gold emission.

---

## 🛡️ 4. Unity Catalog Governance Model

All platform assets reside within the isolated catalog `customer360_dev`:

| Schema | Purpose | Storage / Types |
| :--- | :--- | :--- |
| `staging` | Staging landing zones | Managed Unity Catalog Volume (`source_files`) |
| `bronze` | Raw append-only ingested data | Delta Lake tables with metadata columns |
| `silver` | Conformed, deduplicated business entities | Delta Lake tables with primary keys |
| `gold` | Dimensions, facts, SCD2, Customer 360 views | Delta Lake tables & analytical views |
| `control` | Pipeline run state, watermarks, audit trails | Delta Lake control tables |
| `quarantine` | Malformed, duplicate, or orphaned records | Delta Lake quarantine tables |
