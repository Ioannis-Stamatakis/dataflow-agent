-- dbt model: models/marts/mart_revenue.sql
SELECT
    order_date,
    SUM(net_revenue) AS total_revenue
FROM {{ ref('fct_orders') }}
GROUP BY order_date
