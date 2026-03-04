-- dbt model: models/marts/fct_customer_orders.sql
-- Fixture for testing the dbt test generator

{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

order_items AS (
    SELECT * FROM {{ ref('int_order_items') }}
),

aggregated AS (
    SELECT
        o.order_id,
        o.customer_id,
        c.customer_name,
        c.email,
        c.country,
        o.order_date,
        o.status,
        COUNT(oi.line_item_id)                        AS line_item_count,
        SUM(oi.quantity)                              AS total_quantity,
        SUM(oi.quantity * oi.unit_price)              AS gross_revenue,
        SUM(oi.quantity * oi.unit_price * oi.discount_pct) AS total_discount,
        SUM(oi.quantity * oi.unit_price) - SUM(oi.quantity * oi.unit_price * oi.discount_pct)
                                                      AS net_revenue,
        o.created_at,
        o.updated_at

    FROM orders o
    JOIN customers c    ON o.customer_id = c.customer_id
    JOIN order_items oi ON o.order_id    = oi.order_id

    GROUP BY
        o.order_id,
        o.customer_id,
        c.customer_name,
        c.email,
        c.country,
        o.order_date,
        o.status,
        o.created_at,
        o.updated_at
)

SELECT * FROM aggregated
