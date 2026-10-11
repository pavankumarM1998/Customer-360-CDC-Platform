# Databricks notebook source
# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 00 — Project Setup
# MAGIC
# MAGIC **Purpose**
# MAGIC
# MAGIC Creates the reusable infrastructure for the Customer360 + CDC project.
# MAGIC
# MAGIC **Creates**
# MAGIC - Catalog: `customer360_dev`
# MAGIC - Schemas: `staging`, `bronze`, `silver`, `gold`, `control`, `quarantine`
# MAGIC - Unity Catalog Volume: `customer360_dev.staging.source_files`
# MAGIC - Landing folders for all 8 entities:
# MAGIC   - initial
# MAGIC   - incremental
# MAGIC   - archive
# MAGIC   - rejected
# MAGIC - Core control tables
# MAGIC
# MAGIC **Run frequency**
# MAGIC
# MAGIC Run once when setting up a new Databricks workspace.
# MAGIC Safe to rerun.

# COMMAND ----------

CATALOG = "customer360_dev"

SCHEMAS = [
    "staging",
    "bronze",
    "silver",
    "gold",
    "control",
    "quarantine"
]

ENTITIES = [
    "customers",
    "addresses",
    "subscriptions",
    "products",
    "orders",
    "order_items",
    "payments",
    "support_tickets"
]

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")

for schema in SCHEMAS:
    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}"
    )

print("Catalog and schemas created.")

# COMMAND ----------

spark.sql(
    f"""
    CREATE VOLUME IF NOT EXISTS
    {CATALOG}.staging.source_files
    """
)

BASE_PATH = (
    f"/Volumes/{CATALOG}/staging/source_files"
)

print(BASE_PATH)

# COMMAND ----------

for entity in ENTITIES:

    for folder in [
        "initial",
        "incremental",
        "archive",
        "rejected"
    ]:

        path = f"{BASE_PATH}/{entity}/{folder}"

        dbutils.fs.mkdirs(path)

        print(f"Ready: {path}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Control Tables
# MAGIC
# MAGIC These tables store operational metadata only.
# MAGIC They are not business-facing Gold tables.

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS
    {CATALOG}.control.file_processing_status
    (
        entity_name STRING,
        file_path STRING,
        file_name STRING,
        load_type STRING,
        pipeline_run_id STRING,
        processing_status STRING,
        row_count BIGINT,
        failure_reason STRING,
        processed_at TIMESTAMP,
        archived_at TIMESTAMP
    )
    USING DELTA
    """
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS
    {CATALOG}.control.pipeline_runs
    (
        pipeline_run_id STRING,
        pipeline_name STRING,
        entity_name STRING,
        load_type STRING,
        run_status STRING,
        start_timestamp TIMESTAMP,
        end_timestamp TIMESTAMP,
        last_successful_layer STRING,
        failure_layer STRING,
        failure_reason STRING
    )
    USING DELTA
    """
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS
    {CATALOG}.control.reconciliation_results
    (
        pipeline_run_id STRING,
        entity_name STRING,
        layer_name STRING,
        source_count BIGINT,
        target_count BIGINT,
        quarantine_count BIGINT,
        duplicate_count BIGINT,
        reconciliation_status STRING,
        reconciliation_timestamp TIMESTAMP,
        failure_reason STRING
    )
    USING DELTA
    """
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS
    {CATALOG}.control.watermark
    (
        entity_name STRING,
        last_processed_timestamp TIMESTAMP,
        last_processed_file STRING,
        load_status STRING,
        pipeline_run_id STRING,
        updated_at TIMESTAMP
    )
    USING DELTA
    """
)

# COMMAND ----------

print("Customer360 project setup completed successfully.")

display(
    spark.sql(
        f"SHOW SCHEMAS IN {CATALOG}"
    )
)