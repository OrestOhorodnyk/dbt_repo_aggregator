{{ config(materialized='view') }}

SELECT
  o.id,
  o.customer_id,
  o.amount,
  o.order_date,
  c.country
FROM {{ ref('orders') }} o
LEFT JOIN {{ ref('customers') }} c
  ON o.customer_id = c.id
