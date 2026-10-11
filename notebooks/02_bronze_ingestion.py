# Databricks notebook source
# DBTITLE 1,One reusable Bronze notebook for all entities
# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 02 — Parameterized Bronze Ingestion
# MAGIC
# MAGIC **Purpose**
# MAGIC
# MAGIC Loads one source entity into a durable Bronze Delta table.
# MAGIC
# MAGIC **Design**
# MAGIC - source business columns remain strings
# MAGIC - lineage metadata is added
# MAGIC - already processed source files are skipped
# MAGIC - supports initial and incremental loads
# MAGIC - writes file processing state to Control
# MAGIC
# MAGIC **Parameters**
# MAGIC - `ENTITY`
# MAGIC - `LOAD_TYPE`
# MAGIC - optional `PIPELINE_RUN_ID`

# COMMAND ----------

import uuid

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text(
    "ENTITY",
    "customers"
)

dbutils.widgets.dropdown(
    "LOAD_TYPE",
    "INITIAL",
    [
        "INITIAL",
        "INCREMENTAL"
    ]
)

dbutils.widgets.text(
    "PIPELINE_RUN_ID",
    ""
)


# COMMAND ----------


ENTITY = (
    dbutils.widgets
    .get("ENTITY")
    .strip()
    .lower()
)

LOAD_TYPE = (
    dbutils.widgets
    .get("LOAD_TYPE")
    .upper()
)

PIPELINE_RUN_ID = (
    dbutils.widgets
    .get("PIPELINE_RUN_ID")
    .strip()
)

if not PIPELINE_RUN_ID:

    PIPELINE_RUN_ID = (
        str(
            uuid.uuid4()
        )
    )



# COMMAND ----------

CATALOG = "customer360_dev"

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

SOURCE_PATH = (
    f"/Volumes/"
    f"{CATALOG}/"
    f"staging/"
    f"source_files/"
    f"{ENTITY}/"
    f"{LOAD_TYPE.lower()}"
)

TARGET_TABLE = (
    f"{CATALOG}."
    f"bronze."
    f"{ENTITY}"
)

CONTROL_TABLE = (
    f"{CATALOG}."
    f"control."
    f"file_processing_status"
)


# COMMAND ----------

landed_files = [
    item.path
    for item in (
        dbutils.fs.ls(
            SOURCE_PATH
        )
    )
    if item.path
    .lower()
    .endswith(".csv")
]

if not landed_files:

    print(
        f"No {LOAD_TYPE} "
        f"CSV files found "
        f"for {ENTITY}."
    )

    dbutils.notebook.exit(
        "NO_DATA"
    )

print(
    f"Landed files: "
    f"{len(landed_files)}"
)


# COMMAND ----------

processed_files = {
    row["file_path"]

    for row in (

        spark.table(
            CONTROL_TABLE
        )

        .filter(

            (
                F.col(
                    "entity_name"
                )
                == ENTITY
            )

            &

            F.col(
                "processing_status"
            ).isin(
                "BRONZE_LOADED",
                "SILVER_MERGED",
                "ARCHIVED"
            )
        )

        .select(
            "file_path"
        )

        .distinct()

        .collect()
    )
}


# COMMAND ----------

new_files = [
    path
    for path in landed_files
    if path not in processed_files
]

if not new_files:

    print(
        "All landed files "
        "were already processed."
    )

    dbutils.notebook.exit(
        "NO_NEW_FILES"
    )

print(
    f"New files: "
    f"{len(new_files)}"
)


# COMMAND ----------

bronze_df = (

    spark.read

    .option(
        "header",
        True
    )

    .option(
        "inferSchema",
        False
    )

    .csv(
        new_files
    )

    .withColumn(
        "_source_file",
        F.col(
            "_metadata.file_path"
        )
    )

    .withColumn(
        "_ingestion_timestamp",
        F.current_timestamp()
    )

    .withColumn(
        "_source_system",
        F.lit(
            "EXTERNAL_CSV"
        )
    )

    .withColumn(
        "_load_type",
        F.lit(
            LOAD_TYPE
        )
    )

    .withColumn(
        "_pipeline_run_id",
        F.lit(
            PIPELINE_RUN_ID
        )
    )
)

# COMMAND ----------

row_count = (
    bronze_df.count()
)

print(
    f"Rows to load: "
    f"{row_count:,}"
)


# COMMAND ----------


(
    bronze_df
    .write
    .format(
        "delta"
    )
    .mode(
        "append"
    )
    .option(
        "mergeSchema",
        "true"
    )
    .saveAsTable(
        TARGET_TABLE
    )
)

print(
    f"Bronze table updated: "
    f"{TARGET_TABLE}"
)


# COMMAND ----------


file_control_df = (

    bronze_df

    .groupBy(
        "_source_file"
    )

    .count()

    .withColumn(
        "entity_name",
        F.lit(
            ENTITY
        )
    )

    .withColumn(
        "file_name",
        F.regexp_extract(
            "_source_file",
            r"([^/]+$)",
            1
        )
    )

    .withColumn(
        "load_type",
        F.lit(
            LOAD_TYPE
        )
    )

    .withColumn(
        "pipeline_run_id",
        F.lit(
            PIPELINE_RUN_ID
        )
    )

    .withColumn(
        "processing_status",
        F.lit(
            "BRONZE_LOADED"
        )
    )

    .withColumnRenamed(
        "count",
        "row_count"
    )

    .withColumn(
        "failure_reason",
        F.lit(
            None
        ).cast(
            "string"
        )
    )

    .withColumn(
        "processed_at",
        F.current_timestamp()
    )

    .withColumn(
        "archived_at",
        F.lit(
            None
        ).cast(
            "timestamp"
        )
    )

    .select(
        "entity_name",
        F.col(
            "_source_file"
        ).alias(
            "file_path"
        ),
        "file_name",
        "load_type",
        "pipeline_run_id",
        "processing_status",
        "row_count",
        "failure_reason",
        "processed_at",
        "archived_at"
    )
)


# COMMAND ----------

(
    file_control_df
    .write.format("delta")
    .mode("append")
    .saveAsTable(CONTROL_TABLE)
)


# COMMAND ----------


display(
    file_control_df
)

print(
    f"Bronze ingestion completed "
    f"for {ENTITY}. "
    f"Run ID={PIPELINE_RUN_ID}"
)