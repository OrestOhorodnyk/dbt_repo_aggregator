{{ config(materialized='table') }}

SELECT
  c.id AS customer_id,
  c.first_name,
  c.last_name,
  c.country,
  SUM(oi.total_item_value) AS total_spent,
  COUNT(DISTINCT o.id) AS total_orders
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_order_items') }} oi ON o.id = oi.order_id
JOIN {{ ref('customers') }} c ON o.customer_id = c.id
GROUP BY c.id, c.first_name, c.last_name, c.country
