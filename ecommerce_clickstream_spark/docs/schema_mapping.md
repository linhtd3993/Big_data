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

# Task 1 Focus

Priority Tables:

1. events.csv
2. sessions.csv
3. orders.csv
4. order_items.csv
5. products.csv

Secondary Tables:

1. customers.csv
2. reviews.csv

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
