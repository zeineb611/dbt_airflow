-- tests/unique_orders_order_id.sql
SELECT
    geographicID,
    COUNT(*)
FROM
    {{ ref('dim_geographic') }}
GROUP BY
    region
HAVING
    COUNT(*) > 1
