{{ config(materialized='table') }}

SELECT
  p.category,
  SUM(oi.total_item_value) AS total_revenue,
  COUNT(DISTINCT o.id) AS total_orders
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_order_items') }} oi ON o.id = oi.order_id
JOIN {{ ref('stg_products') }} p ON oi.product_id = p.product_id
GROUP BY p.category
