{{ config(materialized='view') }}

SELECT
  oi.order_item_id,
  oi.order_id,
  oi.product_id,
  p.product_name,
  p.category,
  p.price,
  oi.quantity,
  (oi.quantity * p.price) AS total_item_value
FROM {{ ref('order_items') }} oi
LEFT JOIN {{ ref('products') }} p
  ON oi.product_id = p.product_id
