# Databricks notebook source
dbutils.widgets.text(
    "PIPELINE_RUN_ID",
    ""
)

PIPELINE_RUN_ID = (
    dbutils.widgets
    .get("PIPELINE_RUN_ID")
    .strip()
)

if not PIPELINE_RUN_ID:
    raise ValueError(
        "PIPELINE_RUN_ID is required "
        "for safe incremental archival."
    )

# COMMAND ----------

# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 11 — End-to-End Reconciliation & Archive
# MAGIC
# MAGIC **Purpose**
# MAGIC
# MAGIC Performs the final success gate for the Customer360 pipeline.
# MAGIC
# MAGIC **Responsibilities**
# MAGIC - validate required Gold tables
# MAGIC - validate business-key uniqueness
# MAGIC - validate current customer dimension integrity
# MAGIC - identify only successfully processed incremental files
# MAGIC - archive those files
# MAGIC - avoid duplicate archive copies
# MAGIC - update file-processing control state
# MAGIC
# MAGIC **Critical rule**
# MAGIC
# MAGIC Source files are archived only after Gold reconciliation succeeds.
# MAGIC
# MAGIC If there are no eligible files to archive:
# MAGIC
# MAGIC `dbutils.notebook.exit("NO_FILES_TO_ARCHIVE")`

from pyspark.sql import functions as F

CATALOG = "customer360_dev"

SOURCE_BASE = (
    f"/Volumes/{CATALOG}/"
    f"staging/source_files"
)

CONTROL_TABLE = (
    f"{CATALOG}."
    f"control."
    f"file_processing_status"
)

# COMMAND ----------

GOLD_CONFIG = {

    "customers": {
        "table": "dim_customer",
        "key": "customer_id",
        "scd2": True
    },

    "addresses": {
        "table": "dim_address",
        "key": "address_id"
    },

    "products": {
        "table": "dim_product",
        "key": "product_id"
    },

    "orders": {
        "table": "fact_orders",
        "key": "order_id"
    },

    "order_items": {
        "table": "fact_order_items",
        "key": "order_item_id"
    },

    "payments": {
        "table": "fact_payments",
        "key": "payment_id"
    },

    "subscriptions": {
        "table": "fact_subscriptions",
        "key": "subscription_id"
    },

    "support_tickets": {
        "table": "fact_support_tickets",
        "key": "ticket_id"
    }
}

# COMMAND ----------

reconciliation_rows = []
failures = []

for entity, cfg in GOLD_CONFIG.items():

    table_name = (
        f"{CATALOG}."
        f"gold."
        f"{cfg['table']}"
    )

    business_key = (
        cfg["key"]
    )

# COMMAND ----------

if not spark.catalog.tableExists(
        table_name
    ):

        failures.append(
            f"{entity}: "
            f"Gold table missing "
            f"{table_name}"
        )


gold_df = (
        spark.table(
            table_name
        )
    )

# COMMAND ----------

if business_key not in gold_df.columns:

        failures.append(
            f"{entity}: "
            f"business key "
            f"{business_key} missing"
        )


row_count = (
        gold_df.count()
    )

if row_count == 0:

        failures.append(
            f"{entity}: "
            f"Gold contains zero rows"
        )
if cfg.get(
        "scd2",
        False
    ):

        current_df = (

            gold_df

            .filter(
                F.col(
                    "is_current"
                )
                == True
            )
        )

        current_count = (
            current_df.count()
        )

        duplicate_key_count = (

            current_df

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

        if current_count == 0:

            failures.append(
                "customers: "
                "no current SCD2 rows"
            )

            status = "FAIL"

        elif duplicate_key_count > 0:

            failures.append(
                f"customers: "
                f"{duplicate_key_count} "
                f"customers have multiple "
                f"current versions"
            )

            status = "FAIL"

        else:

            status = "PASS"

        reconciliation_rows.append(
            (
                entity,
                table_name,
                row_count,
                current_count,
                duplicate_key_count,
                status
            )
        )

else:

        duplicate_key_count = (

            gold_df

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
            if duplicate_key_count == 0
            else "FAIL"
        )

        if duplicate_key_count > 0:

            failures.append(
                f"{entity}: "
                f"{duplicate_key_count} "
                f"duplicate Gold keys"
            )

        reconciliation_rows.append(
            (
                entity,
                table_name,
                row_count,
                row_count,
                duplicate_key_count,
                status
            )
        )

# COMMAND ----------

reconciliation_df = (
    spark.createDataFrame(
        reconciliation_rows,
        [
            "entity",
            "gold_table",
            "history_or_total_rows",
            "current_or_total_rows",
            "duplicate_business_keys",
            "status"
        ]
    )
)

display(
    reconciliation_df
)

# COMMAND ----------

if failures:

    print(
        "Gold reconciliation FAILED."
    )

    for failure in failures:

        print(
            f" - {failure}"
        )

    raise ValueError(
        "Gold reconciliation failed. "
        "Archive blocked. "
        + " | ".join(
            failures
        )
    )

print(
    "Gold reconciliation PASS."
)

# COMMAND ----------

if not spark.catalog.tableExists(
    CONTROL_TABLE
):

    print(
        "File processing control table "
        "does not exist."
    )

    dbutils.notebook.exit(
        "NO_FILES_TO_ARCHIVE"
    )


# COMMAND ----------

control_df = (
    spark.table(
        CONTROL_TABLE
    )
)

# COMMAND ----------

archive_candidates_df = (
    control_df

    .filter(
        F.col("load_type")
        .isin("INITIAL", "INCREMENTAL")
    )

    .filter(
        (
            F.col("pipeline_run_id")
            == PIPELINE_RUN_ID
        )
        | (
            F.col("load_type")
            == "INITIAL"
        )
    )

    .filter(
        F.col("processing_status")
        == "BRONZE_LOADED"
    )

    .filter(
        F.col("archived_at")
        .isNull()
    )

    .select(
        "entity_name",
        "file_path",
        "file_name",
        "pipeline_run_id"
    )

    .dropDuplicates(
        [
            "entity_name",
            "file_path"
        ]
    )
)

# COMMAND ----------

if (
    archive_candidates_df
    .limit(1)
    .count()
    == 0
):

    print(
        "No successfully processed "
        "files to archive."
    )

    dbutils.notebook.exit(
        "NO_FILES_TO_ARCHIVE"
    )

# COMMAND ----------

display(
    archive_candidates_df
)

# COMMAND ----------

# DBTITLE 1,Archive loop - merge cell 15 into cell 14
archive_candidates = (
    archive_candidates_df.collect()
)

archive_results = []

for row in archive_candidates:

    entity = (
        row[
            "entity_name"
        ]
    )

    source_file = (
        row[
            "file_path"
        ]
    )

    file_name = (
        row[
            "file_name"
        ]
    )

    archive_path = (
        f"{SOURCE_BASE}/"
        f"{entity}/"
        f"archive/"
        f"{file_name}"
    )

    result = {
        "entity": entity,
        "source_file": source_file,
        "archive_file": archive_path,
        "status": None
    }

    # ----------------------------------------------------------
    # Check if source file exists
    # ----------------------------------------------------------

    try:
        dbutils.fs.ls(source_file)
        source_exists = True
    except Exception:
        source_exists = False

    if not source_exists:

        # Check whether it is already archived

        try:
            dbutils.fs.ls(archive_path)
            result["status"] = "ALREADY_ARCHIVED"
        except Exception:
            result["status"] = "SOURCE_MISSING"

        archive_results.append(result)
        continue

    # ----------------------------------------------------------
    # Avoid duplicate archive file
    # ----------------------------------------------------------

    try:
        dbutils.fs.ls(archive_path)
        archive_exists = True
    except Exception:
        archive_exists = False

    if archive_exists:
        result["status"] = "ALREADY_ARCHIVED"
        archive_results.append(result)
        continue

    # ----------------------------------------------------------
    # Move file to archive
    # ----------------------------------------------------------

    dbutils.fs.mv(source_file, archive_path)
    result["status"] = "ARCHIVED"
    archive_results.append(result)

# COMMAND ----------

# DBTITLE 1,Clear cell 15 (merged into cell 14)
# Cell merged into Cell 14 — archive logic now runs inside the for loop

# COMMAND ----------

archive_result_df = (
    spark.createDataFrame(
        [
            (
                x["entity"],
                x["source_file"],
                x["archive_file"],
                x["status"]
            )
            for x
            in archive_results
        ],
        [
            "entity",
            "source_file",
            "archive_file",
            "archive_status"
        ]
    )
)

display(
    archive_result_df
)

# COMMAND ----------

archive_failures = [

    row

    for row
    in archive_results

    if row[
        "status"
    ]
    == "SOURCE_MISSING"
]

if archive_failures:

    for failure in archive_failures:

        print(
            "Archive failure: "
            f"{failure}"
        )

    raise ValueError(
        f"{len(archive_failures)} "
        f"archive candidates were "
        f"missing from both source "
        f"and archive."
    )

# COMMAND ----------

successful_paths = [

    row[
        "source_file"
    ]

    for row
    in archive_results

    if row[
        "status"
    ]
    in {
        "ARCHIVED",
        "ALREADY_ARCHIVED"
    }
]

# COMMAND ----------

if successful_paths:

    success_df = (
        spark.createDataFrame(
            [
                (
                    path,
                )
                for path
                in successful_paths
            ],
            [
                "file_path"
            ]
        )
    )

    success_df.createOrReplaceTempView(
        "archive_success_paths"
    )

    spark.sql(
        f"""
        MERGE INTO {CONTROL_TABLE} AS target

        USING archive_success_paths AS source

        ON
        target.file_path
        =
        source.file_path

        WHEN MATCHED THEN UPDATE SET

            target.processing_status
                = 'ARCHIVED',

            target.archived_at
                = current_timestamp()
        """
    )

# COMMAND ----------

archived_count = len(
    [
        x
        for x
        in archive_results
        if x[
            "status"
        ]
        == "ARCHIVED"
    ]
)

already_archived_count = len(
    [
        x
        for x
        in archive_results
        if x[
            "status"
        ]
        == "ALREADY_ARCHIVED"
    ]
)

# COMMAND ----------



# COMMAND ----------

print(
    "End-to-end reconciliation "
    "and archive completed."
)

print(
    f"Gold reconciliation : PASS"
)

print(
    f"New files archived  : "
    f"{archived_count}"
)

print(
    f"Already archived    : "
    f"{already_archived_count}"
)