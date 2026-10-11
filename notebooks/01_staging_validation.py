# Databricks notebook source
# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 01 — Staging Validation
# MAGIC
# MAGIC **Purpose**
# MAGIC
# MAGIC Validates source delivery before Bronze ingestion.
# MAGIC
# MAGIC **Checks**
# MAGIC - landing folder accessibility
# MAGIC - CSV presence
# MAGIC - non-empty files
# MAGIC - CSV parseability
# MAGIC - required business-key column
# MAGIC - source row count
# MAGIC - optional strict initial-load row count
# MAGIC
# MAGIC **Parameters**
# MAGIC - `ENTITY` = `ALL` or one entity
# MAGIC - `LOAD_TYPE` = `INITIAL` / `INCREMENTAL`
# MAGIC - `STRICT_INITIAL_COUNTS` = `true` / `false`
# MAGIC
# MAGIC Incremental runs are allowed to contain zero files.

# COMMAND ----------

dbutils.widgets.text(
    "ENTITY",
    "ALL"
)

dbutils.widgets.dropdown(
    "LOAD_TYPE",
    "INITIAL",
    [
        "INITIAL",
        "INCREMENTAL"
    ]
)

dbutils.widgets.dropdown(
    "STRICT_INITIAL_COUNTS",
    "false",
    [
        "false",
        "true"
    ]
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

STRICT_INITIAL_COUNTS = (
    dbutils.widgets
    .get("STRICT_INITIAL_COUNTS")
    .lower()
    == "true"
)

CATALOG = "customer360_dev"

BASE_PATH = (
    f"/Volumes/{CATALOG}/staging/source_files"
)


# COMMAND ----------


# COMMAND ----------

CONFIG = {

    "customers": {
        "key": "customer_id",
        "expected_rows": 1_000_000
    },

    "addresses": {
        "key": "address_id",
        "expected_rows": 1_200_000
    },

    "subscriptions": {
        "key": "subscription_id",
        "expected_rows": 1_000_000
    },

    "products": {
        "key": "product_id",
        "expected_rows": 150_000
    },

    "orders": {
        "key": "order_id",
        "expected_rows": 5_000_000
    },

    "order_items": {
        "key": "order_item_id",
        "expected_rows": 15_000_000
    },

    "payments": {
        "key": "payment_id",
        "expected_rows": 6_000_000
    },

    "support_tickets": {
        "key": "ticket_id",
        "expected_rows": 1_500_000
    }
}

# COMMAND ----------

if ENTITY == "all":

    ENTITIES = list(
        CONFIG.keys()
    )

else:

    ENTITIES = [
        ENTITY
    ]

unknown = [
    entity
    for entity in ENTITIES
    if entity not in CONFIG
]

if unknown:

    raise ValueError(
        f"Unsupported ENTITY values: {unknown}"
    )


# COMMAND ----------


# COMMAND ----------

validation_results = []

for entity in ENTITIES:

    folder = (
        f"{BASE_PATH}/"
        f"{entity}/"
        f"{LOAD_TYPE.lower()}"
    )

    print(
        f"\nValidating {entity}"
    )

    try:

        entries = (
            dbutils.fs.ls(
                folder
            )
        )

    except Exception as exc:

        raise RuntimeError(
            f"{entity}: cannot access "
            f"{folder}"
        ) from exc

    csv_files = [
        item
        for item in entries
        if item.path
        .lower()
        .endswith(".csv")
    ]

    # Incremental folder may legitimately be empty.
    if not csv_files:

        if LOAD_TYPE == "INCREMENTAL":

            validation_results.append(
                (
                    entity,
                    0,
                    0,
                    "NO_DATA"
                )
            )

            continue

        raise ValueError(
            f"{entity}: "
            f"no initial CSV files found"
        )

    # Empty-file check
    empty_files = [
        item.path
        for item in csv_files
        if item.size == 0
    ]

    if empty_files:

        raise ValueError(
            f"{entity}: "
            f"empty CSV files found: "
            f"{empty_files[:5]}"
        )

    paths = [
        item.path
        for item in csv_files
    ]

    # Source values intentionally remain strings.
    df = (
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
            paths
        )
    )

    business_key = (
        CONFIG[entity]["key"]
    )

    if business_key not in df.columns:

        raise ValueError(
            f"{entity}: "
            f"required business key "
            f"'{business_key}' missing. "
            f"Columns={df.columns}"
        )

    actual_rows = (
        df.count()
    )

    if actual_rows == 0:

        raise ValueError(
            f"{entity}: "
            f"source contains zero rows"
        )

    expected_rows = (
        CONFIG[entity]
        ["expected_rows"]
    )

    if (
        STRICT_INITIAL_COUNTS
        and LOAD_TYPE == "INITIAL"
        and actual_rows
        != expected_rows
    ):

        raise ValueError(
            f"{entity}: "
            f"expected "
            f"{expected_rows:,} rows, "
            f"found "
            f"{actual_rows:,}"
        )

    validation_results.append(
        (
            entity,
            len(csv_files),
            actual_rows,
            "PASS"
        )
    )


# COMMAND ----------

# DBTITLE 1,Cell 5

# COMMAND ----------

validation_df = (
    spark.createDataFrame(
        validation_results,
        [
            "entity",
            "file_count",
            "row_count",
            "validation_status"
        ]
    )
)

display(
    validation_df
)

# COMMAND ----------

failed_count = (
    validation_df
    .filter(
        "validation_status = 'FAIL'"
    )
    .count()
)

if failed_count > 0:

    raise ValueError(
        "Staging validation failed."
    )

print(
    "Staging validation completed."
)