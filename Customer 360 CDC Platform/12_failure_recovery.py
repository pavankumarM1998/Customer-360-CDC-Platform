# Databricks notebook source
# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 12 — Failure Recovery Toolkit
# MAGIC
# MAGIC **Purpose**
# MAGIC
# MAGIC Helps diagnose failed pipeline runs and identify the safest recovery point.
# MAGIC
# MAGIC **Provides**
# MAGIC - latest pipeline/control state
# MAGIC - file-processing state
# MAGIC - reconciliation failures
# MAGIC - watermark state
# MAGIC - Delta table history
# MAGIC - optional historical version row counts
# MAGIC - recovery recommendation
# MAGIC - RESTORE command generation
# MAGIC
# MAGIC **Safety**
# MAGIC
# MAGIC This notebook does NOT automatically:
# MAGIC - restore a table
# MAGIC - delete data
# MAGIC - rerun another notebook
# MAGIC - move source files
# MAGIC - advance watermarks
# MAGIC
# MAGIC It only diagnoses and recommends.

from pyspark.sql import functions as F

dbutils.widgets.text(
    "ENTITY",
    "customers"
)

dbutils.widgets.text(
    "PIPELINE_RUN_ID",
    ""
)

dbutils.widgets.text(
    "TABLE_NAME",
    ""
)

dbutils.widgets.text(
    "VERSION_AS_OF",
    ""
)

# COMMAND ----------

ENTITY = (
    dbutils.widgets
    .get("ENTITY")
    .strip()
    .lower()
)

PIPELINE_RUN_ID = (
    dbutils.widgets
    .get("PIPELINE_RUN_ID")
    .strip()
)

TABLE_NAME = (
    dbutils.widgets
    .get("TABLE_NAME")
    .strip()
)

VERSION_AS_OF = (
    dbutils.widgets
    .get("VERSION_AS_OF")
    .strip()
)

CATALOG = "customer360_dev"


# COMMAND ----------

VALID_ENTITIES = {
    "customers",
    "addresses",
    "subscriptions",
    "products",
    "orders",
    "order_items",
    "payments",
    "support_tickets"
}

if ENTITY not in VALID_ENTITIES:

    raise ValueError(
        f"Unsupported ENTITY={ENTITY}"
    )

# COMMAND ----------

PIPELINE_RUNS_TABLE = (
    f"{CATALOG}."
    f"control."
    f"pipeline_runs"
)

FILE_STATUS_TABLE = (
    f"{CATALOG}."
    f"control."
    f"file_processing_status"
)

RECON_TABLE = (
    f"{CATALOG}."
    f"control."
    f"reconciliation_results"
)

WATERMARK_TABLE = (
    f"{CATALOG}."
    f"control."
    f"watermark"
)

# COMMAND ----------

if spark.catalog.tableExists(
    PIPELINE_RUNS_TABLE
):

    pipeline_runs_df = (
        spark.table(
            PIPELINE_RUNS_TABLE
        )
    )

    if PIPELINE_RUN_ID:

        pipeline_runs_df = (
            pipeline_runs_df
            .filter(
                F.col(
                    "pipeline_run_id"
                )
                == PIPELINE_RUN_ID
            )
        )

    else:

        pipeline_runs_df = (
            pipeline_runs_df
            .filter(
                F.col(
                    "entity_name"
                )
                == ENTITY
            )
        )

    print(
        "Latest pipeline run state:"
    )

    display(
        pipeline_runs_df
        .orderBy(
            F.col(
                "start_timestamp"
            )
            .desc_nulls_last()
        )
        .limit(20)
    )

else:

    print(
        "pipeline_runs table "
        "does not exist yet."
    )

# COMMAND ----------

if spark.catalog.tableExists(
    FILE_STATUS_TABLE
):

    file_df = (
        spark.table(
            FILE_STATUS_TABLE
        )
        .filter(
            F.col(
                "entity_name"
            )
            == ENTITY
        )
    )

    if PIPELINE_RUN_ID:

        file_df = (
            file_df
            .filter(
                F.col(
                    "pipeline_run_id"
                )
                == PIPELINE_RUN_ID
            )
        )

    print(
        f"File state for {ENTITY}:"
    )

    display(
        file_df
        .orderBy(
            F.col(
                "processed_at"
            )
            .desc_nulls_last()
        )
        .limit(100)
    )

else:

    print(
        "file_processing_status "
        "table does not exist."
    )


# COMMAND ----------

if spark.catalog.tableExists(
    RECON_TABLE
):

    recon_df = (
        spark.table(
            RECON_TABLE
        )
        .filter(
            F.col(
                "entity_name"
            )
            == ENTITY
        )
    )

    if PIPELINE_RUN_ID:

        recon_df = (
            recon_df
            .filter(
                F.col(
                    "pipeline_run_id"
                )
                == PIPELINE_RUN_ID
            )
        )

    print(
        "Recent reconciliation results:"
    )

    display(
        recon_df
        .orderBy(
            F.col(
                "reconciliation_timestamp"
            )
            .desc_nulls_last()
        )
        .limit(100)
    )

else:

    print(
        "reconciliation_results "
        "table does not exist."
    )


# COMMAND ----------

if spark.catalog.tableExists(
    WATERMARK_TABLE
):

    watermark_df = (
        spark.table(
            WATERMARK_TABLE
        )
        .filter(
            F.col(
                "entity_name"
            )
            == ENTITY
        )
    )

    print(
        f"Watermark state for {ENTITY}:"
    )

    display(
        watermark_df
    )

else:

    print(
        "watermark table does not exist."
    )


# COMMAND ----------

BRONZE_TABLE = (
    f"{CATALOG}.bronze.{ENTITY}"
)

SILVER_TABLE = (
    f"{CATALOG}.silver.{ENTITY}"
)

# COMMAND ----------

layer_status = []

for layer_name, table_name in [
    (
        "BRONZE",
        BRONZE_TABLE
    ),
    (
        "SILVER",
        SILVER_TABLE
    )
]:

    if spark.catalog.tableExists(
        table_name
    ):

        row_count = (
            spark.table(
                table_name
            )
            .count()
        )

        layer_status.append(
            (
                layer_name,
                table_name,
                True,
                row_count
            )
        )

    else:

        layer_status.append(
            (
                layer_name,
                table_name,
                False,
                None
            )
        )

# COMMAND ----------

layer_status_df = (
    spark.createDataFrame(
        layer_status,
        [
            "layer_name",
            "table_name",
            "exists",
            "row_count"
        ]
    )
)
display(
    layer_status_df
)

# COMMAND ----------

GOLD_TABLES = {

    "customers":
        "dim_customer",

    "addresses":
        "dim_address",

    "subscriptions":
        "fact_subscriptions",

    "products":
        "dim_product",

    "orders":
        "fact_orders",

    "order_items":
        "fact_order_items",

    "payments":
        "fact_payments",

    "support_tickets":
        "fact_support_tickets"
}

# COMMAND ----------

gold_table = (
    f"{CATALOG}."
    f"gold."
    f"{GOLD_TABLES[ENTITY]}"
)

# COMMAND ----------

if spark.catalog.tableExists(
    gold_table
):

    gold_count = (
        spark.table(
            gold_table
        )
        .count()
    )

    print(
        f"Gold table: "
        f"{gold_table}"
    )

    print(
        f"Gold rows : "
        f"{gold_count:,}"
    )

else:

    print(
        f"Gold table missing: "
        f"{gold_table}"
    )

# COMMAND ----------

bronze_exists = (
    spark.catalog.tableExists(
        BRONZE_TABLE
    )
)

silver_exists = (
    spark.catalog.tableExists(
        SILVER_TABLE
    )
)

gold_exists = (
    spark.catalog.tableExists(
        gold_table
    )
)


# COMMAND ----------

recommendation = None

if not bronze_exists:

    recommendation = (
        "Restart from STAGING / BRONZE. "
        "Bronze table is missing."
    )

elif (
    bronze_exists
    and
    not silver_exists
):

    recommendation = (
        "Bronze exists but Silver is missing. "
        "Resume from Silver processing."
    )

elif (
    silver_exists
    and
    not gold_exists
):

    recommendation = (
        "Silver exists but Gold is missing. "
        "Run reconciliation, SCD2 if applicable, "
        "then rebuild Gold."
    )

else:

    recommendation = (
        "Bronze, Silver, and Gold exist. "
        "Inspect reconciliation/control state. "
        "If data is correct, resume from the "
        "first downstream task that did not complete."
    )

# COMMAND ----------

print(
    "RECOVERY RECOMMENDATION"
)

print(
    "-----------------------"
)

print(
    recommendation
)

# COMMAND ----------

if TABLE_NAME:

    if not spark.catalog.tableExists(
        TABLE_NAME
    ):

        raise ValueError(
            f"Table does not exist: "
            f"{TABLE_NAME}"
        )

    print(
        f"Delta history for "
        f"{TABLE_NAME}"
    )

    history_df = (
        spark.sql(
            f"""
            DESCRIBE HISTORY
            {TABLE_NAME}
            """
        )
    )

    display(
        history_df
        .limit(50)
    )

# COMMAND ----------

if TABLE_NAME:

    current_df = (
        spark.table(
            TABLE_NAME
        )
    )

    current_count = (
        current_df.count()
    )

    print(
        f"Current row count: "
        f"{current_count:,}"
    )

# COMMAND ----------

if (
    TABLE_NAME
    and
    VERSION_AS_OF
):

    try:

        version_number = int(
            VERSION_AS_OF
        )

    except ValueError:

        raise ValueError(
            "VERSION_AS_OF must be "
            "an integer Delta version."
        )

    historical_df = (

        spark.read

        .format(
            "delta"
        )

        .option(
            "versionAsOf",
            version_number
        )

        .table(
            TABLE_NAME
        )
    )

    historical_count = (
        historical_df.count()
    )

    print(
        f"Version {version_number} "
        f"row count: "
        f"{historical_count:,}"
    )

# COMMAND ----------

if (
    TABLE_NAME
    and
    VERSION_AS_OF
):

    print()
    print(
        "Candidate restore command:"
    )

    print()

    print(
        f"RESTORE TABLE "
        f"{TABLE_NAME} "
        f"TO VERSION AS OF "
        f"{VERSION_AS_OF};"
    )

    print()

    print(
        "Review history and row counts "
        "before executing this manually."
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
                "entity_name"
            )
            == ENTITY
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
        pending_archive_df.count()
    )

    print(
        f"Incremental files not archived: "
        f"{pending_archive_count}"
    )

    if pending_archive_count > 0:

        display(
            pending_archive_df
        )

# COMMAND ----------

print()
print(
    "RECOVERY RULES"
)
print(
    "=============="
)

print(
    "1. If staging failed -> "
    "fix source delivery and restart from staging."
)

print(
    "2. If Bronze failed -> "
    "rerun Bronze only for the affected entity/file."
)

print(
    "3. If Bronze reconciliation failed -> "
    "repair Bronze/input before Silver."
)

print(
    "4. If Silver failed -> "
    "reuse valid Bronze and rerun Silver."
)

print(
    "5. If Silver reconciliation failed -> "
    "do not advance watermark."
)

print(
    "6. If Gold failed -> "
    "keep source files unarchived and "
    "rerun from Gold/SCD2 as appropriate."
)

print(
    "7. If archive failed after Gold success -> "
    "rerun archive only."
)

print(
    "8. Use Delta RESTORE only when "
    "the table itself is incorrect, "
    "not as the default recovery method."
)

# COMMAND ----------

print()
print(
    "Failure recovery analysis complete."
)