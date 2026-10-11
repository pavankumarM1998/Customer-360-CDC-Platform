# Databricks notebook source
# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 08 — Watermark Update
# MAGIC
# MAGIC **Purpose**
# MAGIC
# MAGIC Advances the entity watermark only after:
# MAGIC
# MAGIC - incremental Bronze ingestion succeeded
# MAGIC - CDC classification succeeded
# MAGIC - CDC merge succeeded
# MAGIC - Silver reconciliation succeeded
# MAGIC
# MAGIC **Important rule**
# MAGIC
# MAGIC If there are no new incremental rows, the watermark must NOT advance.
# MAGIC
# MAGIC **Parameters**
# MAGIC - `ENTITY`
# MAGIC - optional `PIPELINE_RUN_ID`

from pyspark.sql import functions as F
from delta.tables import DeltaTable

dbutils.widgets.text(
    "ENTITY",
    "customers"
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

PIPELINE_RUN_ID = (
    dbutils.widgets
    .get("PIPELINE_RUN_ID")
    .strip()
)

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


# COMMAND ----------

BRONZE_TABLE = (
    f"{CATALOG}."
    f"bronze."
    f"{ENTITY}"
)

WATERMARK_TABLE = (
    f"{CATALOG}."
    f"control."
    f"watermark"
)


# COMMAND ----------

if not spark.catalog.tableExists(
    BRONZE_TABLE
):

    print(
        f"{ENTITY}: "
        f"Bronze table does not exist."
    )

    dbutils.notebook.exit(
        "NO_NEW_FILES"
    )

# COMMAND ----------

bronze_df = (
    spark.table(
        BRONZE_TABLE
    )
)

# COMMAND ----------

if "_load_type" not in bronze_df.columns:

    raise ValueError(
        f"{ENTITY}: "
        f"_load_type missing from Bronze."
    )

# COMMAND ----------

incremental_df = (

    bronze_df

    .filter(
        F.col(
            "_load_type"
        )
        == "INCREMENTAL"
    )
)


# COMMAND ----------

if (
    incremental_df
    .limit(1)
    .count()
    == 0
):

    print(
        f"{ENTITY}: "
        f"No incremental rows. "
        f"Watermark not advanced."
    )

    dbutils.notebook.exit(
        "NO_NEW_FILES"
    )


# COMMAND ----------

if (
    PIPELINE_RUN_ID
    and "_pipeline_run_id"
    in incremental_df.columns
):

    current_batch_df = (

        incremental_df

        .filter(
            F.col(
                "_pipeline_run_id"
            )
            == PIPELINE_RUN_ID
        )
    )

    print(
        "Watermark scope selected "
        f"using PIPELINE_RUN_ID="
        f"{PIPELINE_RUN_ID}"
    )

else:

    latest_ingestion_ts = (

        incremental_df

        .agg(
            F.max(
                "_ingestion_timestamp"
            )
            .alias(
                "latest_ts"
            )
        )

        .first()[
            "latest_ts"
        ]
    )

    current_batch_df = (

        incremental_df

        .filter(
            F.col(
                "_ingestion_timestamp"
            )
            ==
            F.lit(
                latest_ingestion_ts
            )
        )
    )

    print(
        "Watermark scope selected "
        f"using latest ingestion batch: "
        f"{latest_ingestion_ts}"
    )

# COMMAND ----------

if (
    current_batch_df
    .limit(1)
    .count()
    == 0
):

    print(
        f"{ENTITY}: "
        f"No rows found for the "
        f"current incremental batch."
    )

    dbutils.notebook.exit(
        "NO_NEW_FILES"
    )

# COMMAND ----------

watermark_row = (

    current_batch_df

    .agg(

        F.max(
            "_ingestion_timestamp"
        )
        .alias(
            "last_processed_timestamp"
        ),

        F.max(
            "_source_file"
        )
        .alias(
            "last_processed_file"
        ),

        F.max(
            "_pipeline_run_id"
        )
        .alias(
            "pipeline_run_id"
        )
    )

    .first()
)

# COMMAND ----------

last_processed_timestamp = (
    watermark_row[
        "last_processed_timestamp"
    ]
)

last_processed_file = (
    watermark_row[
        "last_processed_file"
    ]
)

resolved_run_id = (
    PIPELINE_RUN_ID
    or watermark_row[
        "pipeline_run_id"
    ]
)


# COMMAND ----------

if last_processed_timestamp is None:

    print(
        f"{ENTITY}: "
        f"No valid timestamp found. "
        f"Watermark not advanced."
    )

    dbutils.notebook.exit(
        "NO_NEW_FILES"
    )

# COMMAND ----------

existing_timestamp = None

if spark.catalog.tableExists(
    WATERMARK_TABLE
):

    existing_rows = (

        spark.table(
            WATERMARK_TABLE
        )

        .filter(
            F.col(
                "entity_name"
            )
            == ENTITY
        )

        .select(
            "last_processed_timestamp"
        )

        .limit(1)

        .collect()
    )

    if existing_rows:

        existing_timestamp = (

            existing_rows[0][
                "last_processed_timestamp"
            ]
        )


# COMMAND ----------

if (
    existing_timestamp is not None
    and
    last_processed_timestamp
    <= existing_timestamp
):

    print(
        f"{ENTITY}: "
        f"Existing watermark is already "
        f"at or beyond current batch."
    )

    print(
        f"Existing: "
        f"{existing_timestamp}"
    )

    print(
        f"Current: "
        f"{last_processed_timestamp}"
    )

    dbutils.notebook.exit(
        "WATERMARK_ALREADY_CURRENT"
    )

# COMMAND ----------

watermark_df = (

    spark.createDataFrame(
        [
            (
                ENTITY,
                last_processed_timestamp,
                last_processed_file,
                "SUCCESS",
                resolved_run_id
            )
        ],
        [
            "entity_name",
            "last_processed_timestamp",
            "last_processed_file",
            "load_status",
            "pipeline_run_id"
        ]
    )

    .withColumn(
        "updated_at",
        F.current_timestamp()
    )
)

display(
    watermark_df
)

# COMMAND ----------

if not spark.catalog.tableExists(
    WATERMARK_TABLE
):

    (
        watermark_df

        .write

        .format(
            "delta"
        )

        .mode(
            "overwrite"
        )

        .saveAsTable(
            WATERMARK_TABLE
        )
    )

else:

    watermark_delta = (
        DeltaTable.forName(
            spark,
            WATERMARK_TABLE
        )
    )

    (
        watermark_delta

        .alias(
            "target"
        )

        .merge(

            watermark_df
            .alias(
                "source"
            ),

            (
                "target.entity_name "
                "= "
                "source.entity_name"
            )
        )

        .whenMatchedUpdateAll()

        .whenNotMatchedInsertAll()

        .execute()
    )

# COMMAND ----------

final_watermark_df = (

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

display(
    final_watermark_df
)


# COMMAND ----------

print(
    "Watermark updated successfully."
)

print(
    f"ENTITY                  : {ENTITY}"
)

print(
    f"LAST PROCESSED TIMESTAMP: "
    f"{last_processed_timestamp}"
)

print(
    f"LAST PROCESSED FILE     : "
    f"{last_processed_file}"
)

print(
    f"PIPELINE RUN ID         : "
    f"{resolved_run_id}"
)