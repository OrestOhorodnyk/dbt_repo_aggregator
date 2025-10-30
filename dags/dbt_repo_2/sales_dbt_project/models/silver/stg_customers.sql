{{ config(materialized='view') }}

select
  customer_id,
  first_name,
  last_name,
  country
from {{ ref('customers') }}
