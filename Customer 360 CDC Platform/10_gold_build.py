# Databricks notebook source
# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 10 — Gold Build
# MAGIC
# MAGIC **Purpose**
# MAGIC
# MAGIC Builds business-facing Gold dimensions and facts from trusted Silver data.
# MAGIC
# MAGIC **Gold outputs**
# MAGIC - `dim_address`
# MAGIC - `dim_product`
# MAGIC - `fact_orders`
# MAGIC - `fact_order_items`
# MAGIC - `fact_payments`
# MAGIC - `fact_subscriptions`
# MAGIC - `fact_support_tickets`
# MAGIC
# MAGIC `dim_customer` is maintained separately by `09_customer_scd2`.
# MAGIC
# MAGIC **Important**
# MAGIC
# MAGIC Gold must not expose technical pipeline metadata.
# MAGIC
# MAGIC If a required Silver table has no rows, the notebook fails.
# MAGIC If all required Silver tables are healthy, Gold is rebuilt idempotently.

from pyspark.sql import functions as F

CATALOG = "customer360_dev"

TECHNICAL_COLUMNS = {
    "_source_file",
    "_source_system",
    "_load_type",
    "_ingestion_timestamp",
    "_pipeline_run_id",
    "_quarantined_at",
    "_quarantine_reason",
    "_cdc_entity",
    "_cdc_classified_at"
}


# COMMAND ----------

GOLD_MAPPING = {

    "addresses": {
        "target": "dim_address",
        "key": "address_id"
    },

    "products": {
        "target": "dim_product",
        "key": "product_id"
    },

    "orders": {
        "target": "fact_orders",
        "key": "order_id"
    },

    "order_items": {
        "target": "fact_order_items",
        "key": "order_item_id"
    },

    "payments": {
        "target": "fact_payments",
        "key": "payment_id"
    },

    "subscriptions": {
        "target": "fact_subscriptions",
        "key": "subscription_id"
    },

    "support_tickets": {
        "target": "fact_support_tickets",
        "key": "ticket_id"
    }
}

# COMMAND ----------

validation_failures = []

for source_entity, config in GOLD_MAPPING.items():

    silver_table = (
        f"{CATALOG}."
        f"silver."
        f"{source_entity}"
    )

    if not spark.catalog.tableExists(
        silver_table
    ):

        validation_failures.append(
            f"{silver_table} missing"
        )

        continue

    silver_df = (
        spark.table(
            silver_table
        )
    )

    if (
        silver_df
        .limit(1)
        .count()
        == 0
    ):

        validation_failures.append(
            f"{silver_table} contains zero rows"
        )

    key = config["key"]

    if key not in silver_df.columns:

        validation_failures.append(
            f"{silver_table}: "
            f"business key {key} missing"
        )


# COMMAND ----------

if validation_failures:

    print(
        "Gold build blocked."
    )

    for failure in validation_failures:

        print(
            f" - {failure}"
        )

    raise ValueError(
        "Gold prerequisite validation failed: "
        + " | ".join(
            validation_failures
        )
    )

# COMMAND ----------

def business_columns(df):

    return [

        column_name

        for column_name
        in df.columns

        if (
            column_name
            not in TECHNICAL_COLUMNS

            and

            not column_name.startswith("_")
        )
    ]

# COMMAND ----------

# DBTITLE 1,Gold build loop
gold_results = []

for source_entity, config in GOLD_MAPPING.items():

    source_table = (
        f"{CATALOG}."
        f"silver."
        f"{source_entity}"
    )

    target_table = (
        f"{CATALOG}."
        f"gold."
        f"{config['target']}"
    )

    key = (
        config["key"]
    )

    print(
        f"\nBuilding {target_table}"
    )

    source_df = (
        spark.table(
            source_table
        )
    )

    selected_columns = (
        business_columns(
            source_df
        )
    )

    gold_df = (

        source_df

        .select(
            *selected_columns
        )
    )

    duplicate_count = (

        gold_df

        .groupBy(
            key
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

    if duplicate_count > 0:

        raise ValueError(
            f"{source_entity}: "
            f"{duplicate_count} duplicate "
            f"business keys before Gold write."
        )

    row_count = (
        gold_df.count()
    )

    (
        gold_df

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
            target_table
        )
    )

    gold_results.append(
        (
            source_entity,
            target_table,
            row_count,
            duplicate_count,
            "PASS"
        )
    )

    print(
        f"Completed: "
        f"{target_table} "
        f"rows={row_count:,}"
    )

# COMMAND ----------

DIM_CUSTOMER = (
    f"{CATALOG}.gold.dim_customer"
)

if not spark.catalog.tableExists(
    DIM_CUSTOMER
):

    raise ValueError(
        "Gold dim_customer is missing. "
        "Run 09_customer_scd2 first."
    )

# COMMAND ----------

dim_customer_df = (
    spark.table(
        DIM_CUSTOMER
    )
)

current_customer_count = (

    dim_customer_df

    .filter(
        F.col(
            "is_current"
        )
        == True
    )

    .count()
)

if current_customer_count == 0:

    raise ValueError(
        "dim_customer contains no "
        "current customer versions."
    )

# COMMAND ----------

gold_tables = [
    f"{CATALOG}.gold.dim_customer",
    f"{CATALOG}.gold.dim_address",
    f"{CATALOG}.gold.dim_product",
    f"{CATALOG}.gold.fact_orders",
    f"{CATALOG}.gold.fact_order_items",
    f"{CATALOG}.gold.fact_payments",
    f"{CATALOG}.gold.fact_subscriptions",
    f"{CATALOG}.gold.fact_support_tickets"
]

metadata_failures = []

for table_name in gold_tables:

    df = (
        spark.table(
            table_name
        )
    )

    bad_columns = [

        column_name

        for column_name
        in df.columns

        if (
            column_name
            in TECHNICAL_COLUMNS

            or

            column_name.startswith(
                "_pipeline"
            )

            or

            column_name.startswith(
                "_source"
            )

            or

            column_name.startswith(
                "_ingestion"
            )

            or

            column_name.startswith(
                "_load"
            )
        )
    ]

    if bad_columns:

        metadata_failures.append(
            f"{table_name}: "
            f"{bad_columns}"
        )

# COMMAND ----------

if metadata_failures:

    raise ValueError(
        "Technical metadata leaked "
        "into Gold: "
        + " | ".join(
            metadata_failures
        )
    )

# COMMAND ----------

result_df = (
    spark.createDataFrame(
        gold_results,
        [
            "source_entity",
            "gold_table",
            "row_count",
            "duplicate_business_keys",
            "status"
        ]
    )
)

display(
    result_df
)

# COMMAND ----------

print(
    "Gold build completed successfully."
)

print(
    f"Current customers: "
    f"{current_customer_count:,}"
)

for table_name in gold_tables:

    count = (
        spark.table(
            table_name
        )
        .count()
    )

    print(
        f"{table_name}: "
        f"{count:,}"
    )