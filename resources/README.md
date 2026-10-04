# Platform Folder Resources

This directory contains local data folder resource structures and resource provisioning scripts for the **Customer 360 CDC Platform**.

## Directory Layout (`resources/data/`)

```
resources/data/
├── staging/source_files/    # Staging area with initial/, incremental/, archive/ for 8 domain entities
├── bronze/                  # Raw/Ingested Delta Lake storage (customers, orders, addresses, etc.)
├── silver/                  # Cleaned/Validated Delta Lake storage
├── gold/                    # Aggregated/Dimensional Delta Lake storage (dim_customer, fact_orders, agg_customer_360)
├── control/                 # Audit logging, watermark, and CDC control tables
└── checkpoints/             # Streaming / CDC checkpoint directories
```

## Included Provisioning Scripts (`resources/scripts/`)

- [`setup_folder_resources.py`](file:///C:/Users/pavan/.gemini/antigravity-ide/scratch/Customer-360-CDC-Platform/resources/scripts/setup_folder_resources.py): Generates all 60 directory resources locally with `.gitkeep` files.
- [`databricks_create_resources.sql`](file:///C:/Users/pavan/.gemini/antigravity-ide/scratch/Customer-360-CDC-Platform/resources/scripts/databricks_create_resources.sql): Unity Catalog SQL DDL for creating catalog (`customer360_dev`), schemas (`staging`, `bronze`, `silver`, `gold`, `control`), and volumes.
