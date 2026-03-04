-- dbt model: models/marts/fct_orders.sql
-- This model has bugs that dataflow-agent should detect

{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

WITH order_items AS (
    SELECT * FROM {{ ref('int_order_items') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
)

SELECT
    oi.order_id,
    oi.customer_id,
    c.customer_name,
    c.customer_segment,
    oi.order_date,
    COUNT(oi.line_item_id) AS line_item_count,
    SUM(oi.quantity * oi.unit_price) AS gross_revenue,
    -- BUG: column is 'discount_pct' not 'discount_amount'
    SUM(oi.discount_amount) AS total_discount,
    SUM(oi.quantity * oi.unit_price) - SUM(oi.discount_amount) AS net_revenue,
    MIN(oi.order_date) OVER (PARTITION BY oi.customer_id) AS first_order_date

FROM order_items oi
-- Missing JOIN condition hint
LEFT JOIN customers c ON oi.customer_id = c.customer_id

GROUP BY
    oi.order_id,
    oi.customer_id,
    c.customer_name,
    c.customer_segment,
    oi.order_date
