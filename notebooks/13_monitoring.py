# Databricks notebook source
# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 13 — Operational Monitoring
# MAGIC
# MAGIC **Purpose**
# MAGIC
# MAGIC Provides a single operational view of the Customer360 platform.
# MAGIC
# MAGIC **Monitors**
# MAGIC - latest watermarks
# MAGIC - source-file processing status
# MAGIC - files waiting for archive
# MAGIC - recent pipeline failures
# MAGIC - reconciliation failures
# MAGIC - quarantine volumes
# MAGIC - Bronze / Silver / Gold row counts
# MAGIC - duplicate business-key health
# MAGIC
# MAGIC **Important**
# MAGIC
# MAGIC This notebook is observational.
# MAGIC
# MAGIC It does NOT:
# MAGIC - modify pipeline state
# MAGIC - advance watermarks
# MAGIC - archive files
# MAGIC - restore Delta tables
# MAGIC
# MAGIC Missing optional tables are reported instead of failing the notebook.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "customer360_dev"

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

BUSINESS_KEYS = {
    "customers": "customer_id",
    "addresses": "address_id",
    "subscriptions": "subscription_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id",
    "support_tickets": "ticket_id"
}

GOLD_TABLES = {
    "customers": "dim_customer",
    "addresses": "dim_address",
    "subscriptions": "fact_subscriptions",
    "products": "dim_product",
    "orders": "fact_orders",
    "order_items": "fact_order_items",
    "payments": "fact_payments",
    "support_tickets": "fact_support_tickets"
}

# COMMAND ----------

WATERMARK_TABLE = (
    f"{CATALOG}."
    f"control."
    f"watermark"
)

if spark.catalog.tableExists(
    WATERMARK_TABLE
):

    print(
        "=== WATERMARK STATUS ==="
    )

    watermark_df = (
        spark.table(
            WATERMARK_TABLE
        )
    )

    display(
        watermark_df
        .orderBy(
            "entity_name"
        )
    )

else:

    print(
        "Watermark table not found."
    )

# COMMAND ----------

FILE_STATUS_TABLE = (
    f"{CATALOG}."
    f"control."
    f"file_processing_status"
)

if spark.catalog.tableExists(
    FILE_STATUS_TABLE
):

    file_status_df = (
        spark.table(
            FILE_STATUS_TABLE
        )
    )

    print(
        "=== FILE PROCESSING SUMMARY ==="
    )

    file_summary_df = (
        file_status_df

        .groupBy(
            "entity_name",
            "load_type",
            "processing_status"
        )

        .count()

        .orderBy(
            "entity_name",
            "load_type",
            "processing_status"
        )
    )

    display(
        file_summary_df
    )

else:

    print(
        "File processing status table not found."
    )

# COMMAND ----------

if spark.catalog.tableExists(
    FILE_STATUS_TABLE
):

    pending_archive_df = (

        spark.table(
            FILE_STATUS_TABLE
        )

        .filter(
            F.col(
                "load_type"
            )
            == "INCREMENTAL"
        )

        .filter(
            F.col(
                "processing_status"
            )
            != "ARCHIVED"
        )
    )

    pending_archive_count = (
        pending_archive_df
        .count()
    )

    print(
        f"Files waiting for archive: "
        f"{pending_archive_count}"
    )

    if pending_archive_count > 0:

        display(
            pending_archive_df
            .orderBy(
                F.col(
                    "processed_at"
                )
                .desc_nulls_last()
            )
        )

# COMMAND ----------

PIPELINE_RUNS_TABLE = (
    f"{CATALOG}."
    f"control."
    f"pipeline_runs"
)

if spark.catalog.tableExists(
    PIPELINE_RUNS_TABLE
):

    recent_failures_df = (

        spark.table(
            PIPELINE_RUNS_TABLE
        )

        .filter(
            F.col(
                "run_status"
            )
            .isin(
                "FAILED",
                "ERROR"
            )
        )

        .orderBy(
            F.col(
                "start_timestamp"
            )
            .desc_nulls_last()
        )
    )

    failure_count = (
        recent_failures_df
        .count()
    )

    print(
        f"Recent failed pipeline records: "
        f"{failure_count}"
    )

    if failure_count > 0:

        display(
            recent_failures_df
            .limit(100)
        )

else:

    print(
        "Pipeline runs table not found."
    )


# COMMAND ----------

RECON_TABLE = (
    f"{CATALOG}."
    f"control."
    f"reconciliation_results"
)

if spark.catalog.tableExists(
    RECON_TABLE
):

    recon_failures_df = (

        spark.table(
            RECON_TABLE
        )

        .filter(
            F.col(
                "reconciliation_status"
            )
            != "PASS"
        )

        .orderBy(
            F.col(
                "reconciliation_timestamp"
            )
            .desc_nulls_last()
        )
    )

    recon_failure_count = (
        recon_failures_df
        .count()
    )

    print(
        f"Reconciliation failures: "
        f"{recon_failure_count}"
    )

    if recon_failure_count > 0:

        display(
            recon_failures_df
            .limit(100)
        )

else:

    print(
        "Reconciliation table not found."
    )

# COMMAND ----------

quarantine_results = []

for entity in ENTITIES:

    table_name = (
        f"{CATALOG}."
        f"quarantine."
        f"{entity}"
    )

    if spark.catalog.tableExists(
        table_name
    ):

        row_count = (
            spark.table(
                table_name
            )
            .count()
        )

        quarantine_results.append(
            (
                entity,
                row_count
            )
        )

    else:

        quarantine_results.append(
            (
                entity,
                0
            )
        )

# COMMAND ----------

quarantine_df = (
    spark.createDataFrame(
        quarantine_results,
        [
            "entity",
            "quarantine_rows"
        ]
    )
)

print(
    "=== QUARANTINE COUNTS ==="
)

display(
    quarantine_df
    .orderBy(
        F.col(
            "quarantine_rows"
        )
        .desc()
    )
)

# COMMAND ----------

layer_counts = []

for entity in ENTITIES:

    for layer in [
        "bronze",
        "silver"
    ]:

        table_name = (
            f"{CATALOG}."
            f"{layer}."
            f"{entity}"
        )

        if spark.catalog.tableExists(
            table_name
        ):

            row_count = (
                spark.table(
                    table_name
                )
                .count()
            )

            layer_counts.append(
                (
                    entity,
                    layer.upper(),
                    table_name,
                    row_count,
                    "AVAILABLE"
                )
            )

        else:

            layer_counts.append(
                (
                    entity,
                    layer.upper(),
                    table_name,
                    None,
                    "MISSING"
                )
            )

# COMMAND ----------

layer_counts_df = (
    spark.createDataFrame(
        layer_counts,
        [
            "entity",
            "layer",
            "table_name",
            "row_count",
            "table_status"
        ]
    )
)

print(
    "=== BRONZE / SILVER COUNTS ==="
)

display(
    layer_counts_df
)

# COMMAND ----------

gold_counts = []

for entity, gold_name in GOLD_TABLES.items():

    table_name = (
        f"{CATALOG}."
        f"gold."
        f"{gold_name}"
    )

    if spark.catalog.tableExists(
        table_name
    ):

        df = (
            spark.table(
                table_name
            )
        )

        row_count = (
            df.count()
        )

        if (
            entity == "customers"
            and
            "is_current"
            in df.columns
        ):

            current_count = (
                df.filter(
                    F.col(
                        "is_current"
                    )
                    == True
                )
                .count()
            )

        else:

            current_count = (
                row_count
            )

        gold_counts.append(
            (
                entity,
                table_name,
                row_count,
                current_count,
                "AVAILABLE"
            )
        )

    else:

        gold_counts.append(
            (
                entity,
                table_name,
                None,
                None,
                "MISSING"
            )
        )

# COMMAND ----------

gold_counts_df = (
    spark.createDataFrame(
        gold_counts,
        [
            "entity",
            "gold_table",
            "total_rows",
            "current_rows",
            "table_status"
        ]
    )
)

print(
    "=== GOLD COUNTS ==="
)

display(
    gold_counts_df
)

# COMMAND ----------

duplicate_results = []

for entity in ENTITIES:

    business_key = (
        BUSINESS_KEYS[
            entity
        ]
    )

    silver_table = (
        f"{CATALOG}."
        f"silver."
        f"{entity}"
    )

    if not spark.catalog.tableExists(
        silver_table
    ):

        duplicate_results.append(
            (
                entity,
                "SILVER",
                None,
                "TABLE_MISSING"
            )
        )

        continue

    silver_df = (
        spark.table(
            silver_table
        )
    )

    if business_key not in silver_df.columns:

        duplicate_results.append(
            (
                entity,
                "SILVER",
                None,
                "KEY_MISSING"
            )
        )

        continue

    duplicate_count = (

        silver_df

        .groupBy(
            business_key
        )

        .count()

        .filter(
            F.col(
                "count"
            )
            > 1
        )

        .count()
    )

    status = (
        "PASS"
        if duplicate_count == 0
        else "FAIL"
    )

    duplicate_results.append(
        (
            entity,
            "SILVER",
            duplicate_count,
            status
        )
    )

# COMMAND ----------

duplicate_health_df = (
    spark.createDataFrame(
        duplicate_results,
        [
            "entity",
            "layer",
            "duplicate_business_keys",
            "status"
        ]
    )
)

print(
    "=== DUPLICATE-KEY HEALTH ==="
)

display(
    duplicate_health_df
)

# COMMAND ----------

DIM_CUSTOMER = (
    f"{CATALOG}."
    f"gold."
    f"dim_customer"
)

if spark.catalog.tableExists(
    DIM_CUSTOMER
):

    dim_customer_df = (
        spark.table(
            DIM_CUSTOMER
        )
    )

    if (
        "is_current"
        in dim_customer_df.columns
    ):

        multiple_current_df = (

            dim_customer_df

            .filter(
                F.col(
                    "is_current"
                )
                == True
            )

            .groupBy(
                "customer_id"
            )

            .count()

            .filter(
                F.col(
                    "count"
                )
                > 1
            )
        )

        multiple_current_count = (
            multiple_current_df
            .count()
        )

        print(
            "Customers with multiple "
            f"current SCD2 versions: "
            f"{multiple_current_count}"
        )

        if multiple_current_count > 0:

            display(
                multiple_current_df
                .limit(100)
            )

# COMMAND ----------

health_results = []

# Pending archive
if spark.catalog.tableExists(
    FILE_STATUS_TABLE
):

    pending_files = (

        spark.table(
            FILE_STATUS_TABLE
        )

        .filter(
            F.col(
                "load_type"
            )
            == "INCREMENTAL"
        )

        .filter(
            F.col(
                "processing_status"
            )
            != "ARCHIVED"
        )

        .count()
    )

else:

    pending_files = None

# COMMAND ----------

if spark.catalog.tableExists(
    RECON_TABLE
):

    failed_reconciliations = (

        spark.table(
            RECON_TABLE
        )

        .filter(
            F.col(
                "reconciliation_status"
            )
            != "PASS"
        )

        .count()
    )

else:

    failed_reconciliations = None


# COMMAND ----------

silver_duplicate_failures = (

    duplicate_health_df

    .filter(
        F.col(
            "status"
        )
        == "FAIL"
    )

    .count()
)

# COMMAND ----------

health_results.append(
    (
        "Pending incremental files",
        pending_files
    )
)

health_results.append(
    (
        "Failed reconciliation records",
        failed_reconciliations
    )
)

health_results.append(
    (
        "Silver entities with duplicates",
        silver_duplicate_failures
    )
)

health_results.append(
    (
        "Total quarantine rows",
        sum(
            row[
                "quarantine_rows"
            ]
            for row
            in quarantine_df.collect()
        )
    )
)

# COMMAND ----------

health_df = (
    spark.createDataFrame(
        health_results,
        [
            "metric",
            "value"
        ]
    )
)

print(
    "=== PLATFORM HEALTH SUMMARY ==="
)

display(
    health_df
)

# COMMAND ----------

print(
    "Monitoring completed successfully."
)