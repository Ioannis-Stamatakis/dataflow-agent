-- dbt model: models/reports/rpt_daily_orders.sql
SELECT
    order_date,
    COUNT(order_id) AS order_count
FROM {{ ref('fct_orders') }}
GROUP BY order_date
