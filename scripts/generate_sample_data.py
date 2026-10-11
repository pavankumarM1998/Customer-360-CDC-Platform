"""
Synthetic Sample Data Generator for Customer 360 CDC Platform
Generates realistic initial baseline and incremental CDC CSV datasets.
"""

import os
import csv
from datetime import datetime

SAMPLE_DIR = "sample_data"
os.makedirs(f"{SAMPLE_DIR}/initial", exist_ok=True)
os.makedirs(f"{SAMPLE_DIR}/incremental", exist_ok=True)

# 1. Customers
cust_initial = [
    ["customer_id", "first_name", "last_name", "email", "phone", "date_of_birth", "customer_status", "created_at", "updated_at"],
    ["CUST_0001", "John", "Doe", "john.doe@example.com", "+1-555-0101", "1985-04-12", "ACTIVE", "2024-01-15T10:00:00Z", "2024-01-15T10:00:00Z"],
    ["CUST_0002", "Jane", "Smith", "jane.smith@example.com", "+1-555-0102", "1990-08-23", "ACTIVE", "2024-01-16T11:30:00Z", "2024-01-16T11:30:00Z"],
    ["CUST_0003", "Robert", "Johnson", "r.johnson@example.com", "+1-555-0103", "1978-11-05", "ACTIVE", "2024-01-17T09:15:00Z", "2024-01-17T09:15:00Z"],
    ["CUST_0004", "Emily", "Davis", "emily.davis@example.com", "+1-555-0104", "1995-02-18", "PENDING", "2024-01-18T14:45:00Z", "2024-01-18T14:45:00Z"],
    ["CUST_0005", "Michael", "Brown", "m.brown@example.com", "+1-555-0105", "1982-07-30", "ACTIVE", "2024-01-19T08:20:00Z", "2024-01-19T08:20:00Z"]
]

cust_incremental = [
    ["customer_id", "first_name", "last_name", "email", "phone", "date_of_birth", "customer_status", "created_at", "updated_at"],
    # UPDATE on CUST_0001: status change and new phone (triggers SCD2 change)
    ["CUST_0001", "John", "Doe", "john.doe.new@example.com", "+1-555-9999", "1985-04-12", "PREMIUM", "2024-01-15T10:00:00Z", "2024-02-01T15:00:00Z"],
    # INSERT new customer CUST_0006
    ["CUST_0006", "Sarah", "Wilson", "sarah.w@example.com", "+1-555-0106", "1992-09-14", "ACTIVE", "2024-02-01T16:00:00Z", "2024-02-01T16:00:00Z"]
]

# 2. Addresses
addr_initial = [
    ["address_id", "customer_id", "address_line1", "city", "state", "postal_code", "country", "address_type", "is_primary", "created_at", "updated_at"],
    ["ADDR_0001", "CUST_0001", "100 Market St", "San Francisco", "CA", "94105", "USA", "SHIPPING", "true", "2024-01-15T10:00:00Z", "2024-01-15T10:00:00Z"],
    ["ADDR_0002", "CUST_0002", "200 Broadway", "New York", "NY", "10007", "USA", "SHIPPING", "true", "2024-01-16T11:30:00Z", "2024-01-16T11:30:00Z"],
    ["ADDR_0003", "CUST_0003", "300 Michigan Ave", "Chicago", "IL", "60601", "USA", "SHIPPING", "true", "2024-01-17T09:15:00Z", "2024-01-17T09:15:00Z"],
    ["ADDR_0004", "CUST_0004", "400 Congress Ave", "Austin", "TX", "78701", "USA", "SHIPPING", "true", "2024-01-18T14:45:00Z", "2024-01-18T14:45:00Z"],
    ["ADDR_0005", "CUST_0005", "500 Pike St", "Seattle", "WA", "98101", "USA", "SHIPPING", "true", "2024-01-19T08:20:00Z", "2024-01-19T08:20:00Z"]
]

addr_incremental = [
    ["address_id", "customer_id", "address_line1", "city", "state", "postal_code", "country", "address_type", "is_primary", "created_at", "updated_at"],
    ["ADDR_0006", "CUST_0006", "600 Peachtree St", "Atlanta", "GA", "30308", "USA", "SHIPPING", "true", "2024-02-01T16:00:00Z", "2024-02-01T16:00:00Z"]
]

# 3. Subscriptions
sub_initial = [
    ["subscription_id", "customer_id", "plan_name", "billing_frequency", "amount", "status", "start_date", "renewal_date", "created_at", "updated_at"],
    ["SUB_0001", "CUST_0001", "Enterprise Cloud", "MONTHLY", "299.00", "ACTIVE", "2024-01-15", "2024-02-15", "2024-01-15T10:00:00Z", "2024-01-15T10:00:00Z"],
    ["SUB_0002", "CUST_0002", "Professional", "ANNUAL", "1199.00", "ACTIVE", "2024-01-16", "2025-01-16", "2024-01-16T11:30:00Z", "2024-01-16T11:30:00Z"],
    ["SUB_0003", "CUST_0003", "Starter", "MONTHLY", "49.00", "ACTIVE", "2024-01-17", "2024-02-17", "2024-01-17T09:15:00Z", "2024-01-17T09:15:00Z"],
    ["SUB_0004", "CUST_0005", "Professional", "MONTHLY", "99.00", "ACTIVE", "2024-01-19", "2024-02-19", "2024-01-19T08:20:00Z", "2024-01-19T08:20:00Z"]
]

sub_incremental = [
    ["subscription_id", "customer_id", "plan_name", "billing_frequency", "amount", "status", "start_date", "renewal_date", "created_at", "updated_at"],
    ["SUB_0001", "CUST_0001", "Enterprise Premium", "ANNUAL", "3499.00", "ACTIVE", "2024-01-15", "2025-02-15", "2024-01-15T10:00:00Z", "2024-02-01T15:00:00Z"]
]

# 4. Products
prod_initial = [
    ["product_id", "product_name", "category", "unit_price", "currency", "sku", "status", "created_at", "updated_at"],
    ["PROD_0001", "Lakehouse Analytics Suite", "SOFTWARE", "499.00", "USD", "SKU-SW-001", "AVAILABLE", "2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"],
    ["PROD_0002", "Real-Time CDC Connector", "SOFTWARE", "299.00", "USD", "SKU-SW-002", "AVAILABLE", "2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"],
    ["PROD_0003", "Unity Catalog Governance Pack", "SOFTWARE", "799.00", "USD", "SKU-SW-003", "AVAILABLE", "2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"],
    ["PROD_0004", "Enterprise Support 24x7", "SERVICE", "1500.00", "USD", "SKU-SV-001", "AVAILABLE", "2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"]
]

prod_incremental = [
    ["product_id", "product_name", "category", "unit_price", "currency", "sku", "status", "created_at", "updated_at"],
    ["PROD_0005", "Automated Delta Optimization Agent", "SOFTWARE", "399.00", "USD", "SKU-SW-004", "AVAILABLE", "2024-02-01T00:00:00Z", "2024-02-01T00:00:00Z"]
]

# 5. Orders
order_initial = [
    ["order_id", "customer_id", "shipping_address_id", "order_date", "order_status", "total_amount", "currency", "payment_method", "created_at", "updated_at"],
    ["ORD_0001", "CUST_0001", "ADDR_0001", "2024-01-20T10:15:00Z", "COMPLETED", "798.00", "USD", "CREDIT_CARD", "2024-01-20T10:15:00Z", "2024-01-20T10:15:00Z"],
    ["ORD_0002", "CUST_0002", "ADDR_0002", "2024-01-21T14:20:00Z", "COMPLETED", "499.00", "USD", "BANK_TRANSFER", "2024-01-21T14:20:00Z", "2024-01-21T14:20:00Z"],
    ["ORD_0003", "CUST_0003", "ADDR_0003", "2024-01-22T09:45:00Z", "COMPLETED", "299.00", "USD", "CREDIT_CARD", "2024-01-22T09:45:00Z", "2024-01-22T09:45:00Z"],
    ["ORD_0004", "CUST_0005", "ADDR_0005", "2024-01-23T16:30:00Z", "PROCESSING", "2299.00", "USD", "CREDIT_CARD", "2024-01-23T16:30:00Z", "2024-01-23T16:30:00Z"]
]

order_incremental = [
    ["order_id", "customer_id", "shipping_address_id", "order_date", "order_status", "total_amount", "currency", "payment_method", "created_at", "updated_at"],
    ["ORD_0004", "CUST_0005", "ADDR_0005", "2024-01-23T16:30:00Z", "COMPLETED", "2299.00", "USD", "CREDIT_CARD", "2024-01-23T16:30:00Z", "2024-02-01T12:00:00Z"],
    ["ORD_0005", "CUST_0006", "ADDR_0006", "2024-02-01T17:00:00Z", "COMPLETED", "898.00", "USD", "CREDIT_CARD", "2024-02-01T17:00:00Z", "2024-02-01T17:00:00Z"]
]

# 6. Order Items
items_initial = [
    ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "total_item_price", "discount_amount", "created_at", "updated_at"],
    ["ITEM_0001", "ORD_0001", "PROD_0001", "1", "499.00", "499.00", "0.00", "2024-01-20T10:15:00Z", "2024-01-20T10:15:00Z"],
    ["ITEM_0002", "ORD_0001", "PROD_0002", "1", "299.00", "299.00", "0.00", "2024-01-20T10:15:00Z", "2024-01-20T10:15:00Z"],
    ["ITEM_0003", "ORD_0002", "PROD_0001", "1", "499.00", "499.00", "0.00", "2024-01-21T14:20:00Z", "2024-01-21T14:20:00Z"],
    ["ITEM_0004", "ORD_0003", "PROD_0002", "1", "299.00", "299.00", "0.00", "2024-01-22T09:45:00Z", "2024-01-22T09:45:00Z"],
    ["ITEM_0005", "ORD_0004", "PROD_0003", "1", "799.00", "799.00", "0.00", "2024-01-23T16:30:00Z", "2024-01-23T16:30:00Z"],
    ["ITEM_0006", "ORD_0004", "PROD_0004", "1", "1500.00", "1500.00", "0.00", "2024-01-23T16:30:00Z", "2024-01-23T16:30:00Z"]
]

items_incremental = [
    ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "total_item_price", "discount_amount", "created_at", "updated_at"],
    ["ITEM_0007", "ORD_0005", "PROD_0001", "1", "499.00", "499.00", "0.00", "2024-02-01T17:00:00Z", "2024-02-01T17:00:00Z"],
    ["ITEM_0008", "ORD_0005", "PROD_0005", "1", "399.00", "399.00", "0.00", "2024-02-01T17:00:00Z", "2024-02-01T17:00:00Z"]
]

# 7. Payments
pay_initial = [
    ["payment_id", "order_id", "payment_date", "payment_method", "amount", "currency", "payment_status", "transaction_reference", "created_at", "updated_at"],
    ["PAY_0001", "ORD_0001", "2024-01-20T10:16:00Z", "CREDIT_CARD", "798.00", "USD", "SETTLED", "TXN-CC-9901", "2024-01-20T10:16:00Z", "2024-01-20T10:16:00Z"],
    ["PAY_0002", "ORD_0002", "2024-01-21T14:21:00Z", "BANK_TRANSFER", "499.00", "USD", "SETTLED", "TXN-BT-9902", "2024-01-21T14:21:00Z", "2024-01-21T14:21:00Z"],
    ["PAY_0003", "ORD_0003", "2024-01-22T09:46:00Z", "CREDIT_CARD", "299.00", "USD", "SETTLED", "TXN-CC-9903", "2024-01-22T09:46:00Z", "2024-01-22T09:46:00Z"],
    ["PAY_0004", "ORD_0004", "2024-01-23T16:31:00Z", "CREDIT_CARD", "2299.00", "USD", "AUTHORIZED", "TXN-CC-9904", "2024-01-23T16:31:00Z", "2024-01-23T16:31:00Z"]
]

pay_incremental = [
    ["payment_id", "order_id", "payment_date", "payment_method", "amount", "currency", "payment_status", "transaction_reference", "created_at", "updated_at"],
    ["PAY_0004", "ORD_0004", "2024-01-23T16:31:00Z", "CREDIT_CARD", "2299.00", "USD", "SETTLED", "TXN-CC-9904", "2024-01-23T16:31:00Z", "2024-02-01T12:01:00Z"],
    ["PAY_0005", "ORD_0005", "2024-02-01T17:01:00Z", "CREDIT_CARD", "898.00", "USD", "SETTLED", "TXN-CC-9905", "2024-02-01T17:01:00Z", "2024-02-01T17:01:00Z"]
]

# 8. Support Tickets
tickets_initial = [
    ["ticket_id", "customer_id", "order_id", "subject", "priority", "ticket_status", "channel", "opened_at", "resolved_at", "created_at", "updated_at"],
    ["TCK_0001", "CUST_0001", "ORD_0001", "License activation question", "LOW", "RESOLVED", "PORTAL", "2024-01-20T11:00:00Z", "2024-01-20T12:30:00Z", "2024-01-20T11:00:00Z", "2024-01-20T12:30:00Z"],
    ["TCK_0002", "CUST_0004", "ORD_0002", "Pending account review", "MEDIUM", "OPEN", "EMAIL", "2024-01-19T09:00:00Z", "", "2024-01-19T09:00:00Z", "2024-01-19T09:00:00Z"]
]

tickets_incremental = [
    ["ticket_id", "customer_id", "order_id", "subject", "priority", "ticket_status", "channel", "opened_at", "resolved_at", "created_at", "updated_at"],
    ["TCK_0002", "CUST_0004", "ORD_0002", "Pending account review", "MEDIUM", "RESOLVED", "EMAIL", "2024-01-19T09:00:00Z", "2024-02-01T10:00:00Z", "2024-01-19T09:00:00Z", "2024-02-01T10:00:00Z"],
    ["TCK_0003", "CUST_0006", "ORD_0005", "Connector documentation", "LOW", "OPEN", "PORTAL", "2024-02-01T17:30:00Z", "", "2024-02-01T17:30:00Z", "2024-02-01T17:30:00Z"]
]

datasets = {
    "customers": (cust_initial, cust_incremental),
    "addresses": (addr_initial, addr_incremental),
    "subscriptions": (sub_initial, sub_incremental),
    "products": (prod_initial, prod_incremental),
    "orders": (order_initial, order_incremental),
    "order_items": (items_initial, items_incremental),
    "payments": (pay_initial, pay_incremental),
    "support_tickets": (tickets_initial, tickets_incremental)
}

for entity, (initial_data, incremental_data) in datasets.items():
    # Initial
    with open(f"{SAMPLE_DIR}/initial/{entity}_initial.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(initial_data)
    # Incremental
    with open(f"{SAMPLE_DIR}/incremental/{entity}_incremental.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(incremental_data)

print("Generated sample initial and incremental data files for all 8 entities.")
