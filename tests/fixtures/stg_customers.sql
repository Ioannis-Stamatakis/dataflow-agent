-- dbt model: models/staging/stg_customers.sql
SELECT
    customer_id,
    customer_name,
    customer_segment,
    email
FROM {{ source('raw', 'customers') }}
