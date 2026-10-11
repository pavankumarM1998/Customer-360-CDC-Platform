# Databricks notebook source
# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 14 — Delta Optimization & Maintenance
# MAGIC
# MAGIC **Purpose**
# MAGIC
# MAGIC Performs safe maintenance on important Silver and Gold Delta tables.
# MAGIC
# MAGIC **Responsibilities**
# MAGIC - verify table existence
# MAGIC - run `OPTIMIZE`
# MAGIC - show Delta table details
# MAGIC - optionally show file counts / table size
# MAGIC
# MAGIC **Important**
# MAGIC
# MAGIC This notebook does NOT:
# MAGIC - change business logic
# MAGIC - rewrite CDC rules
# MAGIC - change watermarks
# MAGIC - archive source files
# MAGIC - VACUUM aggressively
# MAGIC
# MAGIC It is safe to run after successful pipeline execution.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "customer360_dev"


# COMMAND ----------

TABLES_TO_OPTIMIZE = [

    f"{CATALOG}.silver.customers",

    f"{CATALOG}.silver.orders",

    f"{CATALOG}.silver.order_items",

    f"{CATALOG}.silver.payments",

    f"{CATALOG}.gold.dim_customer",

    f"{CATALOG}.gold.fact_orders",

    f"{CATALOG}.gold.fact_order_items",

    f"{CATALOG}.gold.fact_payments"
]

# COMMAND ----------

optimization_results = []

for table_name in TABLES_TO_OPTIMIZE:
    print()
    print(f"Checking {table_name}")

    if not spark.catalog.tableExists(table_name):
        print(f"SKIPPED: {table_name} does not exist.")
        optimization_results.append(
            (
                table_name,
                None,
                None,
                "SKIPPED_TABLE_MISSING"
            )
        )
        continue

    row_count = spark.table(table_name).count()
    if row_count == 0:
        print(f"SKIPPED: {table_name} contains zero rows.")
        optimization_results.append(
            (
                table_name,
                0,
                None,
                "SKIPPED_EMPTY_TABLE"
            )
        )
        continue

    detail_before = spark.sql(f"DESCRIBE DETAIL {table_name}").first()
    files_before = (
        detail_before["numFiles"]
        if "numFiles" in detail_before.asDict()
        else None
    )
    size_before = (
        detail_before["sizeInBytes"]
        if "sizeInBytes" in detail_before.asDict()
        else None
    )

    print(f"Rows before: {row_count}")
    print(f"Files before: {files_before}")

    print(f"Running OPTIMIZE {table_name}")
    spark.sql(f"OPTIMIZE {table_name}")

    detail_after = spark.sql(f"DESCRIBE DETAIL {table_name}").first()
    files_after = (
        detail_after["numFiles"]
        if "numFiles" in detail_after.asDict()
        else None
    )
    size_after = (
        detail_after["sizeInBytes"]
        if "sizeInBytes" in detail_after.asDict()
        else None
    )

    print(f"Files after: {files_after}")

    # ----------------------------------------------------------
    # Record result
    # ----------------------------------------------------------
    optimization_results.append(
        (
            table_name,
            row_count,
            files_after,
            "OPTIMIZED"
        )
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Optimization Summary

# COMMAND ----------

optimization_df = (
    spark.createDataFrame(
        optimization_results,
        [
            "table_name",
            "row_count",
            "file_count_after",
            "status"
        ]
    )
)

display(
    optimization_df
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Show Detailed Table Information

# COMMAND ----------

for table_name in TABLES_TO_OPTIMIZE:

    if spark.catalog.tableExists(
        table_name
    ):

        print()
        print(
            f"DETAIL: {table_name}"
        )

        display(
            spark.sql(
                f"""
                DESCRIBE DETAIL
                {table_name}
                """
            )
        )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Optional Delta History
# MAGIC
# MAGIC Useful for checking whether optimization created unexpected table operations.

# COMMAND ----------

for table_name in TABLES_TO_OPTIMIZE:

    if spark.catalog.tableExists(
        table_name
    ):

        print()
        print(
            f"HISTORY: {table_name}"
        )

        display(
            spark.sql(
                f"""
                DESCRIBE HISTORY
                {table_name}
                """
            )
            .limit(10)
        )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Final Summary

# COMMAND ----------

optimized_count = (
    optimization_df
    .filter(
        F.col(
            "status"
        )
        == "OPTIMIZED"
    )
    .count()
)

skipped_count = (
    optimization_df
    .filter(
        F.col(
            "status"
        )
        != "OPTIMIZED"
    )
    .count()
)

print(
    "Optimization completed."
)

print(
    f"Tables optimized: "
    f"{optimized_count}"
)

print(
    f"Tables skipped: "
    f"{skipped_count}"
)