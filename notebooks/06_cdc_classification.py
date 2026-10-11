# Databricks notebook source
# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 06 — CDC Classification
# MAGIC
# MAGIC **Purpose**
# MAGIC
# MAGIC Classifies newly arrived incremental Bronze records against the
# MAGIC current trusted Silver state.
# MAGIC
# MAGIC **CDC classifications**
# MAGIC - `INSERT` — business key does not exist in Silver
# MAGIC - `UPDATE` — business key exists, but business values changed
# MAGIC - `UNCHANGED` — incoming row is identical to trusted Silver state
# MAGIC
# MAGIC **Important**
# MAGIC
# MAGIC Historical incremental rows already processed must NOT be classified again.
# MAGIC
# MAGIC The notebook therefore scopes incoming rows using:
# MAGIC
# MAGIC 1. `PIPELINE_RUN_ID` when supplied
# MAGIC 2. otherwise the entity watermark
# MAGIC 3. if no watermark exists yet, the latest incremental ingestion batch
# MAGIC
# MAGIC **No-data behavior**
# MAGIC
# MAGIC If there are no new incremental rows:
# MAGIC
# MAGIC `dbutils.notebook.exit("NO_NEW_FILES")`
# MAGIC
# MAGIC This is a successful no-op, not a pipeline failure.
# MAGIC
# MAGIC **Parameters**
# MAGIC - `ENTITY`
# MAGIC - optional `PIPELINE_RUN_ID`

from pyspark.sql import functions as F
from pyspark.sql.window import Window

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


# COMMAND ----------

CONFIG = {

    "customers": {
        "key": "customer_id",
        "updated": "updated_at"
    },

    "addresses": {
        "key": "address_id",
        "updated": "updated_at"
    },

    "subscriptions": {
        "key": "subscription_id",
        "updated": "updated_at"
    },

    "products": {
        "key": "product_id",
        "updated": "updated_at"
    },

    "orders": {
        "key": "order_id",
        "updated": "updated_at"
    },

    "order_items": {
        "key": "order_item_id",
        "updated": "updated_at"
    },

    "payments": {
        "key": "payment_id",
        "updated": "updated_at"
    },

    "support_tickets": {
        "key": "ticket_id",
        "updated": "updated_at"
    }
}


# COMMAND ----------

if ENTITY not in CONFIG:

    raise ValueError(
        f"Unsupported ENTITY={ENTITY}"
    )

BUSINESS_KEY = (
    CONFIG[ENTITY]["key"]
)

UPDATED_COLUMN = (
    CONFIG[ENTITY]["updated"]
)

# COMMAND ----------

BRONZE_TABLE = (
    f"{CATALOG}.bronze.{ENTITY}"
)

SILVER_TABLE = (
    f"{CATALOG}.silver.{ENTITY}"
)

CDC_WORK_TABLE = (
    f"{CATALOG}.control."
    f"{ENTITY}_cdc_work"
)

WATERMARK_TABLE = (
    f"{CATALOG}.control.watermark"
)

# COMMAND ----------

if not spark.catalog.tableExists(
    BRONZE_TABLE
):

    print(
        f"Bronze table does not exist: "
        f"{BRONZE_TABLE}"
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

required_metadata = [
    "_load_type",
    "_ingestion_timestamp",
    "_source_file",
    "_pipeline_run_id"
]

missing_metadata = [
    column_name
    for column_name in required_metadata
    if column_name
    not in bronze_df.columns
]

if missing_metadata:

    raise ValueError(
        f"{ENTITY}: Bronze metadata "
        f"columns missing: "
        f"{missing_metadata}"
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
        f"No incremental Bronze rows."
    )

    dbutils.notebook.exit(
        "NO_NEW_FILES"
    )

# COMMAND ----------

watermark_timestamp = None

if PIPELINE_RUN_ID:

    incoming_df = (

        incremental_df

        .filter(
            F.col(
                "_pipeline_run_id"
            )
            == PIPELINE_RUN_ID
        )
    )

    print(
        "CDC scope selected using "
        f"PIPELINE_RUN_ID="
        f"{PIPELINE_RUN_ID}"
    )
else:

    watermark_timestamp = None

    if spark.catalog.tableExists(
        WATERMARK_TABLE
    ):

        watermark_row = (

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

            .orderBy(
                F.col(
                    "updated_at"
                )
                .desc_nulls_last()
            )

            .limit(1)

            .collect()
        )

        if watermark_row:

            watermark_timestamp = (
                watermark_row[0][
                    "last_processed_timestamp"
                ]
            )


# COMMAND ----------

if watermark_timestamp is not None:

        incoming_df = (

            incremental_df

            .filter(
                F.col(
                    "_ingestion_timestamp"
                )
                >
                F.lit(
                    watermark_timestamp
                )
            )
        )

        print(
            "CDC scope selected using "
            f"watermark > "
            f"{watermark_timestamp}"
        )

else:

        # ------------------------------------------------------
        # First incremental execution:
        # process the latest ingestion batch only.
        # ------------------------------------------------------

        latest_ingestion_timestamp = (

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

        incoming_df = (

            incremental_df

            .filter(
                F.col(
                    "_ingestion_timestamp"
                )
                ==
                F.lit(
                    latest_ingestion_timestamp
                )
            )
        )

        print(
            "No previous watermark. "
            "Using latest incremental "
            "ingestion batch: "
            f"{latest_ingestion_timestamp}"
        )


# COMMAND ----------

if (
    incoming_df
    .limit(1)
    .count()
    == 0
):

    print(
        f"{ENTITY}: "
        f"No new files / rows "
        f"to classify."
    )

    dbutils.notebook.exit(
        "NO_NEW_FILES"
    )

# COMMAND ----------

incoming_count = (
    incoming_df.count()
)

print(
    f"Incoming CDC rows: "
    f"{incoming_count:,}"
)


# COMMAND ----------

if BUSINESS_KEY not in incoming_df.columns:

    raise ValueError(
        f"{ENTITY}: "
        f"business key "
        f"{BUSINESS_KEY} "
        f"missing from Bronze"
    )

# COMMAND ----------

order_columns = []

if UPDATED_COLUMN in incoming_df.columns:

    incoming_df = (

        incoming_df

        .withColumn(
            "_cdc_updated_ts",
            F.to_timestamp(
                F.col(
                    UPDATED_COLUMN
                )
            )
        )
    )

    order_columns.append(

        F.col(
            "_cdc_updated_ts"
        )

        .desc_nulls_last()
    )

order_columns.append(

    F.col(
        "_ingestion_timestamp"
    )

    .desc_nulls_last()
)

order_columns.append(

    F.col(
        "_source_file"
    )

    .desc_nulls_last()
)


# COMMAND ----------

cdc_window = (

    Window

    .partitionBy(
        BUSINESS_KEY
    )

    .orderBy(
        *order_columns
    )
)


# COMMAND ----------

incoming_latest_df = (

    incoming_df

    .withColumn(
        "_cdc_row_number",
        F.row_number()
        .over(
            cdc_window
        )
    )

    .filter(
        F.col(
            "_cdc_row_number"
        )
        == 1
    )

    .drop(
        "_cdc_row_number",
        "_cdc_updated_ts"
    )
)


# COMMAND ----------

if not spark.catalog.tableExists(
    SILVER_TABLE
):

    print(
        f"Silver table does not exist. "
        f"All {ENTITY} records "
        f"will be classified as INSERT."
    )

    classified_df = (

        incoming_latest_df

        .withColumn(
            "_change_type",
            F.lit(
                "INSERT"
            )
        )
    )

else:

    silver_df = (
        spark.table(
            SILVER_TABLE
        )
    )

    if BUSINESS_KEY not in silver_df.columns:

        raise ValueError(
            f"{ENTITY}: "
            f"business key "
            f"{BUSINESS_KEY} "
            f"missing from Silver"
        )

# COMMAND ----------

TECHNICAL_COLUMNS = {
        "_source_file",
        "_source_system",
        "_load_type",
        "_ingestion_timestamp",
        "_pipeline_run_id",
        "_quarantined_at",
        "_quarantine_reason"
    }

comparable_columns = [

        column_name

        for column_name
        in incoming_latest_df.columns

        if (
            column_name
            != BUSINESS_KEY

            and

            column_name
            not in TECHNICAL_COLUMNS

            and

            column_name
            in silver_df.columns
        )
    ]

print(
        "Business columns compared:"
    )

for column_name in comparable_columns:

        print(
            f" - {column_name}"
        )

# COMMAND ----------

joined_df = (

        incoming_latest_df
        .alias(
            "source"
        )

        .join(

            silver_df
            .alias(
                "target"
            ),

            F.col(
                f"source.{BUSINESS_KEY}"
            )
            ==
            F.col(
                f"target.{BUSINESS_KEY}"
            ),

            "left"
        )
    )

# COMMAND ----------

if comparable_columns:

        change_expression = None

        for column_name in comparable_columns:

            column_changed = (

                ~F.col(
                    f"source.{column_name}"
                )

                .eqNullSafe(

                    F.col(
                        f"target.{column_name}"
                    )
                )
            )

            if change_expression is None:

                change_expression = (
                    column_changed
                )

            else:

                change_expression = (

                    change_expression

                    |

                    column_changed
                )

else:

        change_expression = (
            F.lit(False)
        )


# COMMAND ----------

source_columns = [

        F.col(
            f"source.{column_name}"
        )

        .alias(
            column_name
        )

        for column_name
        in incoming_latest_df.columns
    ]

classified_df = (

        joined_df

        .select(

            *source_columns,

            F.col(
                f"target.{BUSINESS_KEY}"
            )

            .alias(
                "_target_business_key"
            ),

            change_expression
            .alias(
                "_business_changed"
            )
        )

        .withColumn(

            "_change_type",

            F.when(

                F.col(
                    "_target_business_key"
                )
                .isNull(),

                F.lit(
                    "INSERT"
                )
            )

            .when(

                F.col(
                    "_business_changed"
                ),

                F.lit(
                    "UPDATE"
                )
            )

            .otherwise(

                F.lit(
                    "UNCHANGED"
                )
            )
        )

        .drop(
            "_target_business_key",
            "_business_changed"
        )
    )


# COMMAND ----------

classified_df = (

    classified_df

    .withColumn(
        "_cdc_entity",
        F.lit(
            ENTITY
        )
    )

    .withColumn(
        "_cdc_classified_at",
        F.current_timestamp()
    )
)

# COMMAND ----------


(
    classified_df

    .write

    .format(
        "delta"
    )

    .mode(
        "overwrite"
    )

    .option(
        "overwriteSchema",
        "true"
    )

    .saveAsTable(
        CDC_WORK_TABLE
    )
)

# COMMAND ----------

classification_summary = (

    classified_df

    .groupBy(
        "_change_type"
    )

    .count()

    .orderBy(
        "_change_type"
    )
)

display(
    classification_summary
)

# COMMAND ----------

classified_count = (
    classified_df.count()
)

insert_count = (

    classified_df

    .filter(
        F.col(
            "_change_type"
        )
        == "INSERT"
    )

    .count()
)

update_count = (

    classified_df

    .filter(
        F.col(
            "_change_type"
        )
        == "UPDATE"
    )

    .count()
)

unchanged_count = (

    classified_df

    .filter(
        F.col(
            "_change_type"
        )
        == "UNCHANGED"
    )

    .count()
)


# COMMAND ----------

print(
    "CDC classification completed."
)

print(
    f"ENTITY      : {ENTITY}"
)

print(
    f"INPUT       : {incoming_count:,}"
)

print(
    f"CLASSIFIED  : {classified_count:,}"
)

print(
    f"INSERT      : {insert_count:,}"
)

print(
    f"UPDATE      : {update_count:,}"
)

print(
    f"UNCHANGED   : {unchanged_count:,}"
)

print(
    f"CDC TABLE   : {CDC_WORK_TABLE}"
)