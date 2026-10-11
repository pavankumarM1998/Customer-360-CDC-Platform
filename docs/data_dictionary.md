# Customer 360 CDC Platform Data Dictionary

This document specifies the schemas, primary keys, relationships, and data types across all layers of the `customer360_dev` Lakehouse.

---

## 🏛️ Entity Overview & Business Keys

The platform orchestrates 8 core business domain entities:

| Entity | Primary Business Key | Foreign Keys / Parent Reference | Target Gold Object |
| :--- | :--- | :--- | :--- |
| **`customers`** | `customer_id` | N/A (Root entity) | `dim_customer` (SCD Type 2) |
| **`addresses`** | `address_id` | `customer_id` -> `customers.customer_id` | `dim_address` |
| **`subscriptions`** | `subscription_id` | `customer_id` -> `customers.customer_id` | `fact_subscriptions` |
| **`products`** | `product_id` | N/A (Root reference) | `dim_product` |
| **`orders`** | `order_id` | `customer_id`, `shipping_address_id` | `fact_orders` |
| **`order_items`** | `order_item_id` | `order_id` -> `orders`, `product_id` -> `products` | `fact_order_items` |
| **`payments`** | `payment_id` | `order_id` -> `orders.order_id` | `fact_payments` |
| **`support_tickets`**| `ticket_id` | `customer_id`, `order_id` | `fact_support_tickets` |

---

## 🥉 1. Bronze Layer Tables (`customer360_dev.bronze`)

All incoming CSV data is ingested into Bronze preserving raw string types alongside lineage audit columns:
- `_source_file`: URI of the source file
- `_source_system`: Source ingestion channel
- `_load_type`: `INITIAL` or `INCREMENTAL`
- `_ingestion_timestamp`: Ingestion timestamp
- `_pipeline_run_id`: Run identifier

---

## 🥈 2. Silver Layer Tables (`customer360_dev.silver`)

Cleaned, typed, and deduplicated tables:
- `customers`
- `addresses`
- `subscriptions`
- `products`
- `orders`
- `order_items`
- `payments`
- `support_tickets`

---

## 🥇 3. Gold Layer Models (`customer360_dev.gold`)

- `dim_customer`: SCD Type 2 (`customer_sk`, `effective_from`, `effective_to`, `is_current`)
- `dim_product`, `dim_address`
- `fact_orders`, `fact_order_items`, `fact_payments`, `fact_subscriptions`, `fact_support_tickets`
- `agg_customer_360`: Analytical 360 view
