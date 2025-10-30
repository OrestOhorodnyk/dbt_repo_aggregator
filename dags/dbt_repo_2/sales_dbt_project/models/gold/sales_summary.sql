{{ config(materialized='table') }}

select
  c.country,
  c.first_name || ' ' || c.last_name as customer_name,
  count(distinct s.order_id) as total_orders,
  sum(s.total_value) as total_spent,
  avg(s.total_value) as avg_order_value
from {{ ref('stg_sales') }} s
join {{ ref('stg_customers') }} c
  on s.customer_id = c.customer_id
group by 1,2
order by total_spent desc
