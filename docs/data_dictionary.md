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

### Common Technical Ingestion Metadata Columns
| Column | Type | Description |
| :--- | :--- | :--- |
| `_source_file` | `STRING` | URI / filename from which the record was loaded |
| `_source_system` | `STRING` | Ingestion channel or producer identifier |
| `_load_type` | `STRING` | `INITIAL` or `INCREMENTAL` |
| `_ingestion_timestamp` | `TIMESTAMP` | UTC timestamp when record was appended to Bronze |
| `_pipeline_run_id` | `STRING` | UUID or Databricks job run identifier |

---

## 🥈 2. Silver Layer Tables (`customer360_dev.silver`)

Silver tables enforce data cleanliness, data typing, and strict referential validation:

### `customer360_dev.silver.customers`
| Column | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `customer_id` | `STRING` | No | Primary business identifier |
| `first_name` | `STRING` | Yes | Customer given name |
| `last_name` | `STRING` | Yes | Customer family name |
| `email` | `STRING` | Yes | Normalized email address |
| `phone` | `STRING` | Yes | E.164 formatted phone number |
| `date_of_birth` | `DATE` | Yes | Date of birth |
| `customer_status` | `STRING` | No | `ACTIVE`, `PENDING`, `PREMIUM`, `INACTIVE` |
| `created_at` | `TIMESTAMP` | No | System record creation timestamp |
| `updated_at` | `TIMESTAMP` | No | Business modification timestamp |

### `customer360_dev.silver.addresses`
| Column | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `address_id` | `STRING` | No | Address primary key |
| `customer_id` | `STRING` | No | Foreign key referencing customers |
| `address_line1` | `STRING` | Yes | Street address |
| `city` | `STRING` | Yes | City name |
| `state` | `STRING` | Yes | State / province code |
| `postal_code` | `STRING` | Yes | Postal / ZIP code |
| `country` | `STRING` | Yes | ISO country code |
| `address_type` | `STRING` | Yes | `SHIPPING`, `BILLING` |
| `is_primary` | `BOOLEAN` | Yes | Primary address indicator |
| `created_at` | `TIMESTAMP` | No | Creation timestamp |
| `updated_at` | `TIMESTAMP` | No | Modification timestamp |

### `customer360_dev.silver.products`
| Column | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `product_id` | `STRING` | No | Product primary key |
| `product_name` | `STRING` | No | Product title |
| `category` | `STRING` | Yes | Product category |
| `unit_price` | `DECIMAL(12,2)`| No | Base retail price |
| `currency` | `STRING` | No | ISO currency code (e.g. USD) |
| `sku` | `STRING` | Yes | Inventory stock keeping unit |
| `status` | `STRING` | No | `AVAILABLE`, `DISCONTINUED` |
| `created_at` | `TIMESTAMP` | No | Creation timestamp |
| `updated_at` | `TIMESTAMP` | No | Modification timestamp |

### `customer360_dev.silver.orders`
| Column | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `order_id` | `STRING` | No | Order primary key |
| `customer_id` | `STRING` | No | Customer foreign key |
| `shipping_address_id`| `STRING` | Yes | Address foreign key |
| `order_date` | `TIMESTAMP` | No | Transaction date |
| `order_status` | `STRING` | No | `PROCESSING`, `COMPLETED`, `CANCELLED` |
| `total_amount` | `DECIMAL(14,2)`| No | Total order value |
| `currency` | `STRING` | No | Currency code |
| `payment_method` | `STRING` | Yes | Card, Wire, etc. |
| `created_at` | `TIMESTAMP` | No | Creation timestamp |
| `updated_at` | `TIMESTAMP` | No | Modification timestamp |

---

## 🥇 3. Gold Layer Models (`customer360_dev.gold`)

### `customer360_dev.gold.dim_customer` (SCD Type 2)
| Column | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `customer_sk` | `STRING` | No | Surrogate historical version key |
| `customer_id` | `STRING` | No | Natural customer business key |
| `first_name` | `STRING` | Yes | Customer first name |
| `last_name` | `STRING` | Yes | Customer last name |
| `email` | `STRING` | Yes | Normalized email address |
| `phone` | `STRING` | Yes | Telephone number |
| `customer_status` | `STRING` | No | Status during effective period |
| `effective_from` | `TIMESTAMP` | No | Version validity start timestamp |
| `effective_to` | `TIMESTAMP` | Yes | Version validity end timestamp (`NULL` if current) |
| `is_current` | `BOOLEAN` | No | `true` if currently active version |

### Unified Analytical View: `customer360_dev.gold.agg_customer_360`
Consolidates metrics per customer:
- `total_orders_count`
- `lifetime_spend_amount`
- `active_subscriptions_count`
- `open_support_tickets_count`
- `last_order_timestamp`
- `primary_shipping_city`, `primary_shipping_state`, `primary_shipping_country`
