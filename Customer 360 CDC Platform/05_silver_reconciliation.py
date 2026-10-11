# Databricks notebook source
# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 05 — Silver Reconciliation
# MAGIC
# MAGIC **Purpose**
# MAGIC
# MAGIC Validates trusted Silver data after Silver processing.
# MAGIC
# MAGIC **Checks**
# MAGIC - Silver table exists
# MAGIC - business key exists
# MAGIC - no null / blank business keys
# MAGIC - no duplicate business keys
# MAGIC - Silver contains valid rows when data was actually processed
# MAGIC - Quarantine count is reported
# MAGIC
# MAGIC **Important Incremental Rule**
# MAGIC
# MAGIC If there are no new incremental files / rows, this notebook exits
# MAGIC cleanly with `NO_NEW_FILES`.
# MAGIC
# MAGIC No-data is not considered a reconciliation failure.
# MAGIC
# MAGIC **Parameters**
# MAGIC - `ENTITY` = one entity or `ALL`
# MAGIC - `MODE` = `INITIAL` / `INCREMENTAL`


from pyspark.sql import functions as F


dbutils.widgets.text(
    "ENTITY",
    "ALL"
)

dbutils.widgets.dropdown(
    "MODE",
    "INITIAL",
    [
        "INITIAL",
        "INCREMENTAL"
    ]
)


# COMMAND ----------

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

BUSINESS_KEYS = {

    "customers":
        "customer_id",

    "addresses":
        "address_id",

    "subscriptions":
        "subscription_id",

    "products":
        "product_id",

    "orders":
        "order_id",

    "order_items":
        "order_item_id",

    "payments":
        "payment_id",

    "support_tickets":
        "ticket_id"
}


# COMMAND ----------

if ENTITY == "all":

    ENTITIES = list(
        BUSINESS_KEYS.keys()
    )

else:

    if ENTITY not in BUSINESS_KEYS:

        raise ValueError(
            f"Unsupported ENTITY={ENTITY}"
        )

    ENTITIES = [
        ENTITY
    ]

# COMMAND ----------

entities_with_data = []

for entity in ENTITIES:

    bronze_table = (
        f"{CATALOG}."
        f"bronze."
        f"{entity}"
    )

    if not spark.catalog.tableExists(
        bronze_table
    ):

        if MODE == "INCREMENTAL":

            print(
                f"{entity}: "
                f"Bronze table does not exist. "
                f"Treating as no incremental data."
            )

            continue

        raise ValueError(
            f"{entity}: "
            f"Bronze table missing"
        )

    bronze_df = (
        spark.table(
            bronze_table
        )
    )

    if MODE == "INCREMENTAL":

        if "_load_type" not in bronze_df.columns:

            print(
                f"{entity}: "
                f"_load_type missing. "
                f"No incremental rows identified."
            )

            continue

        incremental_df = (

            bronze_df

            .filter(
                F.col(
                    "_load_type"
                )
                == "INCREMENTAL"
            )
        )

        if (
            incremental_df
            .limit(1)
            .count()
            == 0
        ):

            print(
                f"{entity}: "
                f"No new incremental rows."
            )

            continue

    entities_with_data.append(
        entity
    )

# COMMAND ----------

if (
    MODE == "INCREMENTAL"
    and
    len(entities_with_data) == 0
):

    print(
        "No new files / rows "
        "to reconcile."
    )

    dbutils.notebook.exit(
        "NO_NEW_FILES"
    )

# COMMAND ----------

ENTITIES_TO_RECONCILE = (
    entities_with_data
    if MODE == "INCREMENTAL"
    else ENTITIES
)

print(
    "Entities to reconcile:"
)

for entity in ENTITIES_TO_RECONCILE:

    print(
        f" - {entity}"
    )

# COMMAND ----------

results = []

failures = []

for entity in ENTITIES_TO_RECONCILE:

    business_key = (
        BUSINESS_KEYS[
            entity
        ]
    )

    bronze_table = (
        f"{CATALOG}."
        f"bronze."
        f"{entity}"
    )

    silver_table = (
        f"{CATALOG}."
        f"silver."
        f"{entity}"
    )

    quarantine_table = (
        f"{CATALOG}."
        f"quarantine."
        f"{entity}"
    )

# COMMAND ----------

# DBTITLE 1,Silver table must exist once processing has occurred

if not spark.catalog.tableExists(silver_table):
    failures.append(
            f"{entity}: "
            f"Silver table missing"
    )

silver_df = (
        spark.table(
            silver_table
        )
    )


# COMMAND ----------

# DBTITLE 1,Business key must exist

if spark.catalog.tableExists(silver_table):

    if ( business_key not in silver_df.columns):

        failures.append(
            f"{entity}: "
            f"business key "
            f"{business_key} "
            f"missing from Silver"
        )

# COMMAND ----------

silver_count = (
        silver_df.count()
    )

# COMMAND ----------

null_key_count = (

        silver_df

        .filter(

            F.col(
                business_key
            ).isNull()

            |

            (
                F.trim(
                    F.col(
                        business_key
                    )
                )
                == ""
            )
        )

        .count()
    )


# COMMAND ----------

duplicate_key_count = (

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

# COMMAND ----------

bronze_df = (
        spark.table(
            bronze_table
        )
    )

if MODE == "INCREMENTAL":

    bronze_scope_df = (

        bronze_df

        .filter(
            F.col(
                "_load_type"
            )
            == "INCREMENTAL"
        )
    )

else:

    bronze_scope_df = (
        bronze_df
    )

bronze_count = (
    bronze_scope_df
    .count()
)

# COMMAND ----------


if spark.catalog.tableExists(quarantine_table):

    quarantine_count = (
        spark.table(
            quarantine_table
        )
        .count()
    )

else:

    quarantine_count = 0

# COMMAND ----------

status = "PASS"

reasons = []

if silver_count == 0:

        status = "FAIL"

        reasons.append(
            "Silver contains zero rows"
        )

if null_key_count > 0:

        status = "FAIL"

        reasons.append(
            f"null_keys="
            f"{null_key_count}"
        )

if duplicate_key_count > 0:

        status = "FAIL"

        reasons.append(
            f"duplicate_keys="
            f"{duplicate_key_count}"
        )

results.append(
        (
            entity,
            MODE,
            bronze_count,
            silver_count,
            quarantine_count,
            null_key_count,
            duplicate_key_count,
            status,
            "; ".join(
                reasons
            )
        )
    )

if status == "FAIL":

        failures.append(
            f"{entity}: "
            + "; ".join(
                reasons
            )
        )

# COMMAND ----------

result_df = (
    spark.createDataFrame(
        results,
        [
            "entity",
            "mode",
            "bronze_rows",
            "silver_rows",
            "quarantine_rows",
            "null_business_keys",
            "duplicate_business_keys",
            "status",
            "failure_reason"
        ]
    )
)

display(
    result_df
)

# COMMAND ----------


from pyspark.sql import functions as F

audit_df = (

    result_df

    .withColumn(
        "pipeline_run_id",
        F.lit(
            None
        ).cast(
            "string"
        )
    )

    .withColumn(
        "layer_name",
        F.lit(
            "SILVER"
        )
    )

    .withColumnRenamed(
        "bronze_rows",
        "source_count"
    )

    .withColumnRenamed(
        "silver_rows",
        "target_count"
    )

    .withColumnRenamed(
        "duplicate_business_keys",
        "duplicate_count"
    )

    .withColumnRenamed(
        "status",
        "reconciliation_status"
    )

    .withColumn(
        "reconciliation_timestamp",
        F.current_timestamp()
    )

    .select(
        "pipeline_run_id",
        "entity",
        "layer_name",
        "source_count",
        "target_count",
        "quarantine_rows",
        "duplicate_count",
        "reconciliation_status",
        "reconciliation_timestamp",
        "failure_reason"
    )

    .withColumnRenamed(
        "entity",
        "entity_name"
    )

    .withColumnRenamed(
        "quarantine_rows",
        "quarantine_count"
    )
)

# COMMAND ----------

(
    audit_df

    .write

    .format(
        "delta"
    )

    .mode(
        "append"
    )

    .saveAsTable(
        f"{CATALOG}."
        f"control."
        f"reconciliation_results"
    )
)


# COMMAND ----------

if failures:

    print(
        "Silver reconciliation FAILED."
    )

    for failure in failures:

        print(
            f" - {failure}"
        )

    raise ValueError(

        "Silver reconciliation "
        "failed: "

        + " | ".join(
            failures
        )
    )


# COMMAND ----------


print(
    "Silver reconciliation PASS."
)