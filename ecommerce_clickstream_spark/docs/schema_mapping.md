# Schema Mapping

## customers

Primary Key:
- customer_id

Columns:
- customer_id
- name
- email
- country
- age
- signup_date
- marketing_opt_in

Role:
Customer Dimension

---

## sessions

Primary Key:
- session_id

Foreign Key:
- customer_id -> customers.customer_id

Role:
Session Dimension

---

## events

Primary Key:
- event_id

Foreign Key:
- session_id -> sessions.session_id
- product_id -> products.product_id

Role:
Clickstream Fact Table

---

## orders

Primary Key:
- order_id

Foreign Key:
- customer_id -> customers.customer_id

Role:
Order Fact Table

---

## order_items

Primary Key:
- (order_id, product_id)

Foreign Key:
- order_id -> orders.order_id
- product_id -> products.product_id

Role:
Order Detail Fact Table

---

## products

Primary Key:
- product_id

Role:
Product Dimension

---

## reviews

Primary Key:
- review_id

Foreign Key:
- order_id -> orders.order_id
- product_id -> products.product_id

Role:
Review Fact Table

---

# Main Relationships

customers.customer_id
    -> sessions.customer_id
    -> events.session_id

customers.customer_id
    -> orders.customer_id
    -> order_items.order_id

products.product_id
    -> events.product_id
    -> order_items.product_id
    -> reviews.product_id

# Task Mapping

## Task 1 - Data Ingestion And Cleaning

Priority Tables:

1. events.csv
2. sessions.csv
3. orders.csv
4. order_items.csv
5. products.csv

Secondary Tables:

1. customers.csv
2. reviews.csv

Main output layer:

- data/processed/events_cleaned_parquet
- data/processed/customers_cleaned_parquet
- data/processed/sessions_cleaned_parquet
- data/processed/products_cleaned_parquet
- data/processed/orders_cleaned_parquet
- data/processed/order_items_cleaned_parquet

## Task 2 - Key Metrics

Most active users:

- Input: events_cleaned_parquet, sessions_cleaned_parquet, customers_cleaned_parquet
- Join path: events.session_id -> sessions.session_id -> customers.customer_id
- Output: data/output/task2_metrics/most_active_users/

Top products:

- Input: events_cleaned_parquet, products_cleaned_parquet
- Join path: events.product_id_int -> products.product_id
- Output: data/output/task2_metrics/top_products/

Peak activity time:

- Input: events_cleaned_parquet
- Time columns: event_hour, day_of_week, event_date
- Output:
  - data/output/task2_metrics/peak_activity_by_hour/
  - data/output/task2_metrics/peak_activity_by_day/
  - data/output/task2_metrics/peak_activity_by_date/

## Task 3 - Category Statistics

Current implementation:

- check_schema.py reads products, order_items, and events processed Parquet tables.
- analysis.py groups products by category.

Main fields:

- products.category
- products.product_id
- products.price_usd
- products.margin_usd

Current aggregation:

- product_count
- avg_price
- min_price
- max_price
- avg_margin

# Data Quality Notes

events.csv contains many null values in:

- product_id
- qty
- cart_size
- payment
- discount_pct
- amount_usd

These null values are expected because different event types contain different attributes.

Cleaning rules must be based on event_type and business logic rather than removing all rows containing null values.
