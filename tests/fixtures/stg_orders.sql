-- dbt model: models/staging/stg_orders.sql
SELECT
    order_id,
    customer_id,
    order_date,
    status
FROM {{ source('raw', 'orders') }}
