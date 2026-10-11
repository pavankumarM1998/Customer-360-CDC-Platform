# Databricks notebook source
# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 04 — Parameterized Silver Entity Processing
# MAGIC
# MAGIC **Purpose**
# MAGIC
# MAGIC Creates trusted Silver state for any Customer360 entity.
# MAGIC
# MAGIC **Behavior**
# MAGIC - trims strings
# MAGIC - validates business keys
# MAGIC - validates important parent-child relationships
# MAGIC - deterministic deduplication
# MAGIC - writes invalid rows to Quarantine
# MAGIC - INITIAL mode creates trusted baseline
# MAGIC - INCREMENTAL mode performs Delta MERGE
# MAGIC - stale updates do not replace newer trusted state
# MAGIC
# MAGIC **Parameters**
# MAGIC - `ENTITY`
# MAGIC - `MODE` = `INITIAL` / `INCREMENTAL`


from pyspark.sql import functions as F

from pyspark.sql.window import Window

from delta.tables import DeltaTable



dbutils.widgets.text(
    "ENTITY",
    "customers"
)

dbutils.widgets.dropdown(
    "MODE",
    "INITIAL",
    [
        "INITIAL",
        "INCREMENTAL"
    ]
)

ENTITY = (
    dbutils.widgets
    .get("ENTITY")
    .strip()
    .lower()
)

MODE = (
    dbutils.widgets
    .get("MODE")
    .upper()
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
        "updated": "updated_at",
        "parent": (
            "customer_id",
            "customers",
            "customer_id"
        )
    },

    "subscriptions": {
        "key": "subscription_id",
        "updated": "updated_at",
        "parent": (
            "customer_id",
            "customers",
            "customer_id"
        )
    },

    "products": {
        "key": "product_id",
        "updated": "updated_at"
    },

    "orders": {
        "key": "order_id",
        "updated": "updated_at",
        "parent": (
            "customer_id",
            "customers",
            "customer_id"
        )
    },

    "order_items": {
        "key": "order_item_id",
        "updated": "updated_at",
        "parent": (
            "order_id",
            "orders",
            "order_id"
        )
    },

    "payments": {
        "key": "payment_id",
        "updated": "updated_at",
        "parent": (
            "order_id",
            "orders",
            "order_id"
        )
    },

    "support_tickets": {
        "key": "ticket_id",
        "updated": "updated_at",
        "parent": (
            "customer_id",
            "customers",
            "customer_id"
        )
    }
}


# COMMAND ----------

if ENTITY not in CONFIG:

    raise ValueError(
        f"Unsupported ENTITY={ENTITY}"
    )

cfg = CONFIG[
    ENTITY
]

SOURCE_TABLE = (
    f"{CATALOG}."
    f"bronze."
    f"{ENTITY}"
)

TARGET_TABLE = (
    f"{CATALOG}."
    f"silver."
    f"{ENTITY}"
)

QUARANTINE_TABLE = (
    f"{CATALOG}."
    f"quarantine."
    f"{ENTITY}"
)


# COMMAND ----------

df = (
    spark.table(
        SOURCE_TABLE
    )
)

if (
    MODE == "INCREMENTAL"
    and "_load_type"
    in df.columns
):

    df = (
        df.filter(
            F.col(
                "_load_type"
            )
            == "INCREMENTAL"
        )
    )


# COMMAND ----------

if (
    df.limit(1)
    .count()
    == 0
):

    print(
        f"No rows to process "
        f"for {ENTITY}, "
        f"MODE={MODE}"
    )

    dbutils.notebook.exit(
        "NO_DATA"
    )


# COMMAND ----------

for (
    column_name,
    data_type
) in df.dtypes:

    if data_type == "string":

        df = (
            df.withColumn(
                column_name,
                F.trim(
                    F.col(
                        column_name
                    )
                )
            )
        )


# COMMAND ----------

business_key = (
    cfg["key"]
)

invalid_key_df = (

    df.filter(

        F.col(
            business_key
        ).isNull()

        |

        (
            F.col(
                business_key
            )
            == ""
        )
    )
)

valid_df = (

    df.filter(

        F.col(
            business_key
        ).isNotNull()

        &

        (
            F.col(
                business_key
            )
            != ""
        )
    )
)


# COMMAND ----------

orphan_df = None

if "parent" in cfg:

    (
        foreign_key_column,
        parent_entity,
        parent_business_key
    ) = cfg[
        "parent"
    ]

    parent_table = (
        f"{CATALOG}."
        f"silver."
        f"{parent_entity}"
    )

    if not spark.catalog.tableExists(
        parent_table
    ):

        raise ValueError(
            f"{ENTITY}: "
            f"required parent "
            f"Silver table missing: "
            f"{parent_table}"
        )

    parent_df = (

        spark.table(
            parent_table
        )

        .select(
            F.col(
                parent_business_key
            )
            .alias(
                "_parent_key"
            )
        )

        .distinct()
    )

    joined_df = (

        valid_df

        .join(

            parent_df,

            valid_df[
                foreign_key_column
            ]
            ==
            parent_df[
                "_parent_key"
            ],

            "left"
        )
    )

    orphan_df = (

        joined_df

        .filter(
            F.col(
                "_parent_key"
            )
            .isNull()
        )

        .drop(
            "_parent_key"
        )
    )

    valid_df = (

        joined_df

        .filter(
            F.col(
                "_parent_key"
            )
            .isNotNull()
        )

        .drop(
            "_parent_key"
        )
    )


# COMMAND ----------


order_columns = []

updated_column = (
    cfg["updated"]
)

if updated_column in valid_df.columns:

    valid_df = (

        valid_df

        .withColumn(
            "_updated_ts",
            F.to_timestamp(
                F.col(
                    updated_column
                )
            )
        )
    )

    order_columns.append(

        F.col(
            "_updated_ts"
        )

        .desc_nulls_last()
    )

order_columns.append(

    F.col(
        "_ingestion_timestamp"
    )

    .desc_nulls_last()
)

dedupe_window = (

    Window

    .partitionBy(
        business_key
    )

    .orderBy(
        *order_columns
    )
)

trusted_df = (

    valid_df

    .withColumn(
        "_row_number",
        F.row_number()
        .over(
            dedupe_window
        )
    )

    .filter(
        F.col(
            "_row_number"
        )
        == 1
    )

    .drop(
        "_row_number",
        "_updated_ts"
    )
)


# COMMAND ----------

quarantine_df = (

    invalid_key_df

    .withColumn(
        "_quarantine_reason",
        F.lit(
            "MISSING_BUSINESS_KEY"
        )
    )
)

if orphan_df is not None:

    orphan_with_reason = (

        orphan_df

        .withColumn(
            "_quarantine_reason",
            F.lit(
                "ORPHAN_FOREIGN_KEY"
            )
        )
    )

    quarantine_df = (

        quarantine_df

        .unionByName(
            orphan_with_reason,
            allowMissingColumns=True
        )
    )

# COMMAND ----------

quarantine_count = (
    quarantine_df.count()
)

if quarantine_count > 0:

    (
        quarantine_df

        .withColumn(
            "_quarantined_at",
            F.current_timestamp()
        )

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
            QUARANTINE_TABLE
        )
    )


# COMMAND ----------

if (
    MODE == "INITIAL"
    or
    not spark.catalog.tableExists(
        TARGET_TABLE
    )
):

    (
        trusted_df

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
            TARGET_TABLE
        )
    )

else:

    target_delta = (

        DeltaTable.forName(
            spark,
            TARGET_TABLE
        )
    )

    merge_builder = (

        target_delta

        .alias(
            "t"
        )

        .merge(

            trusted_df
            .alias(
                "s"
            ),

            (
                f"t.{business_key} "
                f"= "
                f"s.{business_key}"
            )
        )
    )

    if (
        updated_column
        in trusted_df.columns
    ):

        merge_builder = (

            merge_builder

            .whenMatchedUpdateAll(

                condition=(

                    f"to_timestamp("
                    f"s.{updated_column}"
                    f") "

                    f">= "

                    f"to_timestamp("
                    f"t.{updated_column}"
                    f")"
                )
            )
        )

    else:

        merge_builder = (

            merge_builder

            .whenMatchedUpdateAll()
        )

    (
        merge_builder

        .whenNotMatchedInsertAll()

        .execute()
    )

# COMMAND ----------

trusted_count = (
    trusted_df.count()
)

print(
    f"Silver processing complete"
)

print(
    f"ENTITY = {ENTITY}"
)

print(
    f"MODE = {MODE}"
)

print(
    f"Trusted rows = "
    f"{trusted_count:,}"
)

print(
    f"Quarantine rows = "
    f"{quarantine_count:,}"
)

# COMMAND ----------


display(
    spark.table(
        TARGET_TABLE
    )
    .limit(20)
)