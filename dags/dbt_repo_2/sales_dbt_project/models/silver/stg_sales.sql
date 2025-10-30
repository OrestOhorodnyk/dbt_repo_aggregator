{{ config(materialized='view') }}

select
  order_id,
  customer_id,
  product,
  quantity,
  price,
  order_date,
  quantity * price as total_value
from {{ ref('sales') }}
