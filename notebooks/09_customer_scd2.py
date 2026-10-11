# Databricks notebook source
# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 09 — Customer SCD Type 2
# MAGIC
# MAGIC **Purpose**
# MAGIC
# MAGIC Maintains historical customer versions in Gold.
# MAGIC
# MAGIC **Output**
# MAGIC
# MAGIC `customer360_dev.gold.dim_customer`
# MAGIC
# MAGIC **SCD2 columns**
# MAGIC - `customer_sk`
# MAGIC - `effective_from`
# MAGIC - `effective_to`
# MAGIC - `is_current`
# MAGIC
# MAGIC **Modes**
# MAGIC - `BASELINE`
# MAGIC - `INCREMENTAL`
# MAGIC
# MAGIC **BASELINE**
# MAGIC
# MAGIC Creates the first historical version from trusted Silver customers.
# MAGIC
# MAGIC **INCREMENTAL**
# MAGIC
# MAGIC - finds customer records whose business attributes changed
# MAGIC - expires the existing current row
# MAGIC - inserts a new current version
# MAGIC
# MAGIC **Important**
# MAGIC
# MAGIC Technical pipeline metadata is NOT exposed in Gold.
# MAGIC
# MAGIC If there are no changed customers:
# MAGIC
# MAGIC `dbutils.notebook.exit("NO_CHANGES")`


from pyspark.sql import functions as F
from delta.tables import DeltaTable

dbutils.widgets.dropdown(
    "MODE",
    "BASELINE",
    [
        "BASELINE",
        "INCREMENTAL"
    ]
)

MODE = (
    dbutils.widgets
    .get("MODE")
    .upper()
)

CATALOG = "customer360_dev"

SILVER_CUSTOMERS = (
    f"{CATALOG}.silver.customers"
)

DIM_CUSTOMER = (
    f"{CATALOG}.gold.dim_customer"
)

# COMMAND ----------

if not spark.catalog.tableExists(
    SILVER_CUSTOMERS
):

    raise ValueError(
        f"Required table missing: "
        f"{SILVER_CUSTOMERS}"
    )


# COMMAND ----------

silver_df = (
    spark.table(
        SILVER_CUSTOMERS
    )
)

if (
    silver_df
    .limit(1)
    .count()
    == 0
):

    print(
        "Silver customers contains no rows."
    )

    dbutils.notebook.exit(
        "NO_DATA"
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

business_columns = [
    column_name
    for column_name
    in silver_df.columns
    if column_name
    not in TECHNICAL_COLUMNS
    and not column_name.startswith("_")
]


# COMMAND ----------

if "customer_id" not in business_columns:

    raise ValueError(
        "customer_id missing from "
        "Silver customers."
    )

# COMMAND ----------

duplicate_customers = (

    silver_df

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

duplicate_count = (
    duplicate_customers
    .count()
)

if duplicate_count > 0:

    display(
        duplicate_customers
        .limit(100)
    )

    raise ValueError(
        f"Silver customers contains "
        f"{duplicate_count} duplicate "
        f"customer IDs."
    )

# COMMAND ----------

if (
    MODE == "BASELINE"
    or
    not spark.catalog.tableExists(
        DIM_CUSTOMER
    )
):

    baseline_df = (

        silver_df

        .select(
            *business_columns
        )

        .withColumn(
            "effective_from",
            F.coalesce(
                F.to_timestamp(
                    F.col(
                        "updated_at"
                    )
                )
                if "updated_at"
                in business_columns
                else F.lit(None),
                F.current_timestamp()
            )
        )

        .withColumn(
            "effective_to",
            F.lit(
                None
            ).cast(
                "timestamp"
            )
        )

        .withColumn(
            "is_current",
            F.lit(
                True
            )
        )
    )

    baseline_df = (

            baseline_df

            .withColumn(

                "customer_sk",

                F.sha2(

                    F.concat_ws(

                        "||",

                        F.col(
                            "customer_id"
                        ),

                        F.col(
                            "effective_from"
                        )
                        .cast(
                            "string"
                        )
                    ),

                    256
                )
            )
        )

    final_columns = [

            "customer_sk",
            "customer_id"
        ]

    final_columns += [

            column_name

            for column_name
            in business_columns

            if column_name
            != "customer_id"
        ]

    final_columns += [
            "effective_from",
            "effective_to",
            "is_current"
        ]

    baseline_df = (
            baseline_df
            .select(
                *final_columns
            )
        )

    (
            baseline_df

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
                DIM_CUSTOMER
            )
        )

    baseline_count = (
            baseline_df.count()
        )

    print(
            "Customer SCD2 baseline created."
        )

    print(
            f"Rows: {baseline_count:,}"
        )

    dbutils.notebook.exit(
            "BASELINE_COMPLETE"
        )

# COMMAND ----------

dim_df = (
    spark.table(
        DIM_CUSTOMER
    )
)

# COMMAND ----------

current_duplicate_count = (

    dim_df

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

    .count()
)

if current_duplicate_count > 0:

    raise ValueError(
        f"dim_customer contains "
        f"{current_duplicate_count} customers "
        f"with multiple current versions."
    )


# COMMAND ----------

current_dim_df = (

    dim_df

    .filter(
        F.col(
            "is_current"
        )
        == True
    )
)

# COMMAND ----------

SCD_COLUMNS = {
    "customer_sk",
    "customer_id",
    "effective_from",
    "effective_to",
    "is_current"
}

tracked_columns = [

    column_name

    for column_name
    in business_columns

    if (
        column_name
        not in SCD_COLUMNS

        and

        column_name
        != "updated_at"

        and

        column_name
        in current_dim_df.columns
    )
]


# COMMAND ----------

print(
    "Tracked SCD2 attributes:"
)

for column_name in tracked_columns:

    print(
        f" - {column_name}"
    )

# COMMAND ----------

comparison_df = (

    silver_df
    .select(
        *business_columns
    )
    .alias(
        "source"
    )

    .join(

        current_dim_df
        .alias(
            "target"
        ),

        F.col(
            "source.customer_id"
        )
        ==
        F.col(
            "target.customer_id"
        ),

        "left"
    )
)

# COMMAND ----------

new_customer_df = (

    comparison_df

    .filter(
        F.col(
            "target.customer_id"
        )
        .isNull()
    )

    .select(
        "source.*"
    )
)

# COMMAND ----------

if tracked_columns:

    changed_expression = None

    for column_name in tracked_columns:

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

        if changed_expression is None:

            changed_expression = (
                column_changed
            )

        else:

            changed_expression = (

                changed_expression

                |

                column_changed
            )

else:

    changed_expression = (
        F.lit(
            False
        )
    )

# COMMAND ----------

changed_customer_df = (

    comparison_df

    .filter(

        F.col(
            "target.customer_id"
        )
        .isNotNull()

        &

        changed_expression
    )

    .select(
        "source.*"
    )
)

# COMMAND ----------

new_customer_count = (
    new_customer_df.count()
)

changed_customer_count = (
    changed_customer_df.count()
)

print(
    f"New customers     : "
    f"{new_customer_count:,}"
)

print(
    f"Changed customers : "
    f"{changed_customer_count:,}"
)

# COMMAND ----------

if (
    new_customer_count == 0
    and
    changed_customer_count == 0
):

    print(
        "No new or changed customers."
    )

    dbutils.notebook.exit(
        "NO_CHANGES"
    )

# COMMAND ----------

if "updated_at" in business_columns:

    changed_customer_df = (

        changed_customer_df

        .withColumn(
            "_effective_ts",

            F.coalesce(

                F.to_timestamp(
                    F.col(
                        "updated_at"
                    )
                ),

                F.current_timestamp()
            )
        )
    )

    new_customer_df = (

        new_customer_df

        .withColumn(
            "_effective_ts",

            F.coalesce(

                F.to_timestamp(
                    F.col(
                        "updated_at"
                    )
                ),

                F.current_timestamp()
            )
        )
    )

else:

    changed_customer_df = (

        changed_customer_df

        .withColumn(
            "_effective_ts",
            F.current_timestamp()
        )
    )

    new_customer_df = (

        new_customer_df

        .withColumn(
            "_effective_ts",
            F.current_timestamp()
        )
    )


# COMMAND ----------

changed_ids_df = (

    changed_customer_df

    .select(
        "customer_id",
        "_effective_ts"
    )

    .distinct()
)

# COMMAND ----------

if changed_customer_count > 0:

    dim_delta = (
        DeltaTable.forName(
            spark,
            DIM_CUSTOMER
        )
    )

    (
        dim_delta

        .alias(
            "target"
        )

        .merge(

            changed_ids_df
            .alias(
                "source"
            ),

            (
                "target.customer_id "
                "= "
                "source.customer_id "
                "AND "
                "target.is_current = true"
            )
        )

        .whenMatchedUpdate(

            set={
                "is_current":
                    F.lit(False),

                "effective_to":
                    F.col(
                        "source._effective_ts"
                    )
            }
        )

        .execute()
    )

# COMMAND ----------

changed_new_versions_df = (

    changed_customer_df

    .select(
        *business_columns,
        "_effective_ts"
    )
)

# COMMAND ----------

all_new_versions_df = (

    changed_new_versions_df

    .unionByName(

        new_customer_df
        .select(
            *business_columns,
            "_effective_ts"
        ),

        allowMissingColumns=True
    )
)

# COMMAND ----------

all_new_versions_df = (

    all_new_versions_df

    .withColumn(
        "effective_from",
        F.col(
            "_effective_ts"
        )
    )

    .withColumn(
        "effective_to",
        F.lit(
            None
        ).cast(
            "timestamp"
        )
    )

    .withColumn(
        "is_current",
        F.lit(
            True
        )
    )

    .drop(
        "_effective_ts"
    )
)

# COMMAND ----------

all_new_versions_df = (

    all_new_versions_df

    .withColumn(

        "customer_sk",

        F.sha2(

            F.concat_ws(

                "||",

                F.col(
                    "customer_id"
                ),

                F.col(
                    "effective_from"
                )
                .cast(
                    "string"
                )
            ),

            256
        )
    )
)


# COMMAND ----------

target_columns = (
    spark.table(
        DIM_CUSTOMER
    )
    .columns
)


# COMMAND ----------

all_new_versions_df = (

    all_new_versions_df

    .select(
        *target_columns
    )
)

# COMMAND ----------

new_version_count = (
    all_new_versions_df
    .count()
)

# COMMAND ----------

(
    all_new_versions_df

    .write

    .format(
        "delta"
    )

    .mode(
        "append"
    )

    .saveAsTable(
        DIM_CUSTOMER
    )
)

# COMMAND ----------

final_dim_df = (
    spark.table(
        DIM_CUSTOMER
    )
)

# COMMAND ----------

multiple_current_versions = (

    final_dim_df

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

    .count()
)

if multiple_current_versions > 0:

    raise ValueError(
        f"SCD2 integrity failure: "
        f"{multiple_current_versions} "
        f"customers have multiple "
        f"current versions."
    )

# COMMAND ----------

missing_current_versions = (

    silver_df

    .select(
        "customer_id"
    )

    .distinct()

    .join(

        final_dim_df

        .filter(
            F.col(
                "is_current"
            )
            == True
        )

        .select(
            "customer_id"
        )
        .distinct(),

        "customer_id",

        "left_anti"
    )

    .count()
)

if missing_current_versions > 0:

    raise ValueError(
        f"SCD2 integrity failure: "
        f"{missing_current_versions} "
        f"Silver customers do not have "
        f"a current dimension version."
    )

# COMMAND ----------

history_count = (
    final_dim_df.count()
)

current_count = (

    final_dim_df

    .filter(
        F.col(
            "is_current"
        )
        == True
    )

    .count()
)


# COMMAND ----------

print(
    "Customer SCD2 processing completed."
)

print(
    f"New customers        : "
    f"{new_customer_count:,}"
)

print(
    f"Changed customers    : "
    f"{changed_customer_count:,}"
)

print(
    f"New versions created : "
    f"{new_version_count:,}"
)

print(
    f"Total history rows   : "
    f"{history_count:,}"
)

print(
    f"Current customers    : "
    f"{current_count:,}"
)

# COMMAND ----------

display(
    final_dim_df
    .orderBy(
        F.col(
            "customer_id"
        ),
        F.col(
            "effective_from"
        )
    )
    .limit(50)
)