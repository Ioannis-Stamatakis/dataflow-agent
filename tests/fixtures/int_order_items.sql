-- dbt model: models/intermediate/int_order_items.sql
SELECT
    order_id,
    customer_id,
    line_item_id,
    quantity,
    unit_price,
    discount_pct,
    order_date
FROM {{ ref('stg_orders') }}
