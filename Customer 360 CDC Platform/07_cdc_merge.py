# Databricks notebook source
# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 07 — CDC Merge
# MAGIC
# MAGIC **Purpose**
# MAGIC
# MAGIC Applies classified CDC changes into trusted Silver.
# MAGIC
# MAGIC **Processes**
# MAGIC - `INSERT`
# MAGIC - `UPDATE`
# MAGIC
# MAGIC **Ignores**
# MAGIC - `UNCHANGED`
# MAGIC
# MAGIC **Important no-data rule**
# MAGIC
# MAGIC If there are no classified changes to apply, the notebook exits cleanly:
# MAGIC
# MAGIC `dbutils.notebook.exit("NO_CHANGES")`
# MAGIC
# MAGIC No changes is not a failure.
# MAGIC
# MAGIC **Parameter**
# MAGIC - `ENTITY`

from pyspark.sql import functions as F
from delta.tables import DeltaTable

dbutils.widgets.text(
    "ENTITY",
    "customers"
)

ENTITY = (
    dbutils.widgets
    .get("ENTITY")
    .strip()
    .lower()
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

CDC_WORK_TABLE = (
    f"{CATALOG}."
    f"control."
    f"{ENTITY}_cdc_work"
)

SILVER_TABLE = (
    f"{CATALOG}."
    f"silver."
    f"{ENTITY}"
)

# COMMAND ----------

if not spark.catalog.tableExists(
    CDC_WORK_TABLE
):

    print(
        f"CDC work table does not exist: "
        f"{CDC_WORK_TABLE}"
    )

    print(
        "Nothing to merge."
    )

    dbutils.notebook.exit(
        "NO_CHANGES"
    )


# COMMAND ----------

cdc_df = (
    spark.table(
        CDC_WORK_TABLE
    )
)

# COMMAND ----------

required_columns = [
    BUSINESS_KEY,
    "_change_type"
]

missing_columns = [
    column_name
    for column_name
    in required_columns
    if column_name
    not in cdc_df.columns
]

if missing_columns:

    raise ValueError(
        f"{ENTITY}: "
        f"CDC work table missing "
        f"required columns: "
        f"{missing_columns}"
    )


# COMMAND ----------

changes_df = (

    cdc_df

    .filter(
        F.col(
            "_change_type"
        )
        .isin(
            "INSERT",
            "UPDATE"
        )
    )
)


# COMMAND ----------

if (
    changes_df
    .limit(1)
    .count()
    == 0
):

    print(
        f"{ENTITY}: "
        f"No INSERT or UPDATE "
        f"records to merge."
    )

    dbutils.notebook.exit(
        "NO_CHANGES"
    )


# COMMAND ----------

change_summary = (

    changes_df

    .groupBy(
        "_change_type"
    )

    .count()

    .orderBy(
        "_change_type"
    )
)

display(
    change_summary
)

# COMMAND ----------

CDC_ONLY_COLUMNS = [
    "_change_type",
    "_cdc_entity",
    "_cdc_classified_at"
]

silver_input_df = (
    changes_df
)

for column_name in CDC_ONLY_COLUMNS:

    if column_name in silver_input_df.columns:

        silver_input_df = (
            silver_input_df
            .drop(
                column_name
            )
        )


# COMMAND ----------

if not spark.catalog.tableExists(
    SILVER_TABLE
):

    print(
        f"Silver table does not exist: "
        f"{SILVER_TABLE}"
    )

    print(
        "Creating Silver from current CDC changes."
    )

    (
        silver_input_df

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
            SILVER_TABLE
        )
    )

    inserted_count = (
        silver_input_df.count()
    )

    print(
        f"Created Silver table with "
        f"{inserted_count:,} rows."
    )

    dbutils.notebook.exit(
        "MERGE_COMPLETE"
    )

# COMMAND ----------

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

duplicate_change_keys = (

    silver_input_df

    .groupBy(
        BUSINESS_KEY
    )

    .count()

    .filter(
        F.col(
            "count"
        )
        > 1
    )
)

duplicate_key_count = (
    duplicate_change_keys
    .count()
)

if duplicate_key_count > 0:

    display(
        duplicate_change_keys
        .limit(100)
    )

    raise ValueError(
        f"{ENTITY}: "
        f"CDC merge input contains "
        f"{duplicate_key_count} "
        f"duplicate business keys."
    )


# COMMAND ----------

before_count = (
    silver_df.count()
)

insert_count = (

    changes_df

    .filter(
        F.col(
            "_change_type"
        )
        == "INSERT"
    )

    .count()
)

update_count = (

    changes_df

    .filter(
        F.col(
            "_change_type"
        )
        == "UPDATE"
    )

    .count()
)

print(
    f"Silver rows before merge: "
    f"{before_count:,}"
)

print(
    f"INSERT candidates: "
    f"{insert_count:,}"
)

print(
    f"UPDATE candidates: "
    f"{update_count:,}"
)


# COMMAND ----------

target_delta = (
    DeltaTable.forName(
        spark,
        SILVER_TABLE
    )
)

merge_builder = (

    target_delta

    .alias(
        "target"
    )

    .merge(

        silver_input_df
        .alias(
            "source"
        ),

        (
            f"target.{BUSINESS_KEY} "
            f"= "
            f"source.{BUSINESS_KEY}"
        )
    )
)


# COMMAND ----------

if (
    UPDATED_COLUMN
    in silver_input_df.columns

    and

    UPDATED_COLUMN
    in silver_df.columns
):

    merge_builder = (

        merge_builder

        .whenMatchedUpdateAll(

            condition=(

                f"to_timestamp("
                f"source.{UPDATED_COLUMN}"
                f") "

                f">= "

                f"to_timestamp("
                f"target.{UPDATED_COLUMN}"
                f")"

                f" OR "

                f"target.{UPDATED_COLUMN} "
                f"IS NULL"
            )
        )
    )

else:

    merge_builder = (

        merge_builder

        .whenMatchedUpdateAll()
    )

# COMMAND ----------

(
    merge_builder

    .whenNotMatchedInsertAll()

    .execute()
)


# COMMAND ----------

after_df = (
    spark.table(
        SILVER_TABLE
    )
)

after_count = (
    after_df.count()
)

# COMMAND ----------

post_merge_duplicate_count = (

    after_df

    .groupBy(
        BUSINESS_KEY
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

if post_merge_duplicate_count > 0:

    raise ValueError(
        f"{ENTITY}: "
        f"Silver contains "
        f"{post_merge_duplicate_count} "
        f"duplicate business keys "
        f"after CDC merge."
    )

# COMMAND ----------

maximum_expected_count = (
    before_count
    +
    insert_count
)

if after_count > maximum_expected_count:

    raise ValueError(
        f"{ENTITY}: "
        f"unexpected Silver row growth. "
        f"Before={before_count:,}, "
        f"INSERT candidates={insert_count:,}, "
        f"After={after_count:,}"
    )

if after_count < before_count:

    raise ValueError(
        f"{ENTITY}: "
        f"Silver row count decreased "
        f"during CDC merge. "
        f"Before={before_count:,}, "
        f"After={after_count:,}"
    )

# COMMAND ----------

print(
    "CDC merge completed successfully."
)

print(
    f"ENTITY              : {ENTITY}"
)

print(
    f"INSERT candidates   : {insert_count:,}"
)

print(
    f"UPDATE candidates   : {update_count:,}"
)

print(
    f"Silver before       : {before_count:,}"
)

print(
    f"Silver after        : {after_count:,}"
)

print(
    f"Duplicate keys      : "
    f"{post_merge_duplicate_count:,}"
)

# COMMAND ----------

display(
    after_df
    .limit(20)
)