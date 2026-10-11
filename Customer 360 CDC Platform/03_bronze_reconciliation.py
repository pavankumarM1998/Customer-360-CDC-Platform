# Databricks notebook source
# DBTITLE 1,Bronze quality gate before Silver
# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 03 — Bronze Reconciliation (Quality Gate)
# MAGIC
# MAGIC **Purpose**
# MAGIC
# MAGIC Validates that all Bronze Delta tables have row counts consistent with the control table (`file_processing_status`).
# MAGIC Runs after all bronze ingestion tasks complete and before any silver processing.
# MAGIC
# MAGIC **Design**
# MAGIC - Accepts `ENTITY=ALL` (the only supported value for this notebook)
# MAGIC - Iterates over all 8 bronze tables
# MAGIC - Compares actual Delta row counts against expected counts from the control table
# MAGIC - Raises an error if any mismatch is found so downstream silver tasks are skipped
# MAGIC
# MAGIC **Parameters**
# MAGIC - `ENTITY` (default `ALL`)
# MAGIC - `PIPELINE_RUN_ID` (optional)

# COMMAND ----------

# DBTITLE 1,Imports and widgets
# COMMAND ----------

import uuid

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text(
    "ENTITY",
    "ALL"
)

dbutils.widgets.text(
    "PIPELINE_RUN_ID",
    ""
)

# COMMAND ----------

# DBTITLE 1,Configuration and validation
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

if not PIPELINE_RUN_ID:

    PIPELINE_RUN_ID = (
        str(
            uuid.uuid4()
        )
    )

if ENTITY != "all":

    raise ValueError(
        f"This notebook only supports "
        f"ENTITY=ALL, received "
        f"ENTITY={ENTITY}"
    )

CATALOG = "customer360_dev"

BRONZE_SCHEMA = "bronze"

CONTROL_TABLE = (
    f"{CATALOG}."
    f"control."
    f"file_processing_status"
)

BRONZE_ENTITIES = [
    "customers",
    "addresses",
    "subscriptions",
    "products",
    "orders",
    "order_items",
    "payments",
    "support_tickets"
]

print(
    f"Running bronze reconciliation "
    f"for all entities. "
    f"Pipeline Run ID={PIPELINE_RUN_ID}"
)

# COMMAND ----------

# DBTITLE 1,Build control table summary
# COMMAND ----------

control_summary = {
    row["entity_name"]: {
        "file_count": row["file_count"],
        "expected_row_count": row["expected_row_count"]
    }

    for row in (
        spark.table(
            CONTROL_TABLE
        )
        .filter(
            F.col(
                "processing_status"
            )
            .isin(
                "BRONZE_LOADED",
                "SILVER_MERGED",
                "ARCHIVED"
            )
        )
        .filter(
            F.col(
                "entity_name"
            ).isin(
                BRONZE_ENTITIES
            )
        )
        .groupBy(
            "entity_name"
        )
        .agg(
            F.count_distinct(
                "file_path"
            ).alias(
                "file_count"
            ),
            F.sum(
                "row_count"
            ).alias(
                "expected_row_count"
            )
        )
        .collect()
    )
}

print(
    f"Control table summary: "
    f"{len(control_summary)} "
    f"entities found"
)

# COMMAND ----------

# DBTITLE 1,Reconcile bronze tables
# COMMAND ----------

mismatches = []

for entity in BRONZE_ENTITIES:

    bronze_table = (
        f"{CATALOG}."
        f"{BRONZE_SCHEMA}."
        f"{entity}"
    )

    actual_count = (
        spark.table(
            bronze_table
        )
        .count()
    )

    control_row = (
        control_summary
        .get(entity)
    )

    if control_row is None:

        expected_count = 0
        file_count = 0

    else:

        expected_count = (
            control_row[
                "expected_row_count"
            ]
        )
        file_count = (
            control_row[
                "file_count"
            ]
        )

    match = (
        actual_count
        == expected_count
    )

    status = (
        "PASS"
        if match
        else "FAIL"
    )

    print(
        f"{status} | "
        f"{entity}: "
        f"actual={actual_count:,} "
        f"expected={expected_count:,} "
        f"files={file_count}"
    )

    if not match:

        mismatches.append(
            {
                "entity": entity,
                "actual_count": actual_count,
                "expected_count": expected_count,
                "file_count": file_count
            }
        )

# COMMAND ----------

# DBTITLE 1,Summary and exit
# COMMAND ----------

if mismatches:

    mismatch_details = "\n".join(
        f"  - {m['entity']}: "
        f"actual={m['actual_count']:,} "
        f"vs expected={m['expected_count']:,} "
        f"({m['file_count']} files)"

        for m in mismatches
    )

    raise ValueError(
        f"Bronze reconciliation FAILED "
        f"for {len(mismatches)} "
        f"entity/entities:\n"
        f"{mismatch_details}"
    )

print(
    f"\nBronze reconciliation PASSED "
    f"for all {len(BRONZE_ENTITIES)} "
    f"entities."
)

print(
    f"All bronze table row counts "
    f"match control table records."
)

print(
    f"Pipeline Run ID={PIPELINE_RUN_ID}"
)