# Synthetic Sample Data Catalog

This directory contains test datasets for all 8 business entities processed by the **Customer 360 CDC Platform**.

---

## 📁 Directory Structure

```
sample_data/
├── initial/                        # Baseline initial data for cold start
│   ├── customers_initial.csv
│   ├── addresses_initial.csv
│   ├── subscriptions_initial.csv
│   ├── products_initial.csv
│   ├── orders_initial.csv
│   ├── order_items_initial.csv
│   ├── payments_initial.csv
│   └── support_tickets_initial.csv
│
└── incremental/                    # Incremental CDC change files
    ├── customers_incremental.csv   # Includes UPDATEs (triggering SCD2) & new INSERTs
    ├── addresses_incremental.csv   # New address additions
    ├── subscriptions_incremental.csv # Subscription tier upgrades
    ├── products_incremental.csv    # New product catalog entries
    ├── orders_incremental.csv      # Order status transitions (PROCESSING -> COMPLETED)
    ├── order_items_incremental.csv # Incremental order line items
    ├── payments_incremental.csv    # Payment settlement transitions
    └── support_tickets_incremental.csv # Ticket resolution status updates
```

---

## 🚀 Loading Sample Data into Databricks Volume

To upload these sample files directly into your Unity Catalog landing zones:

```python
source_entities = [
    "customers", "addresses", "subscriptions", "products",
    "orders", "order_items", "payments", "support_tickets"
]

# For Initial Load:
# Upload each entity's initial file into:
# /Volumes/customer360_dev/staging/source_files/{entity}/initial/

# For Incremental CDC Load:
# Upload each entity's incremental file into:
# /Volumes/customer360_dev/staging/source_files/{entity}/incremental/
```
