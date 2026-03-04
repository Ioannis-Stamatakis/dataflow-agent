-- Deliberately bad query for testing dataflow-agent explain
-- Issues:
--   1. SELECT * (fetches all columns)
--   2. LIKE with leading wildcard (can't use index)
--   3. Function in WHERE (prevents index use on order_date)
--   4. Subquery in SELECT (correlated, runs once per row)
--   5. ORDER BY without LIMIT (sorts full result set)
--   6. No filter on the large orders table

SELECT *
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN products p ON o.product_id = p.product_id
WHERE LOWER(c.email) LIKE '%@gmail%'
  AND DATE_TRUNC('month', o.order_date) = '2024-01-01'
  AND o.status NOT IN (
      SELECT DISTINCT status FROM order_blacklist
  )
ORDER BY o.created_at DESC
