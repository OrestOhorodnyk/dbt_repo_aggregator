{{ config(materialized='table') }}

SELECT
  country,
  COUNT(id) AS total_orders,
  SUM(amount) AS total_revenue
FROM {{ ref('stg_orders') }}
GROUP BY country
