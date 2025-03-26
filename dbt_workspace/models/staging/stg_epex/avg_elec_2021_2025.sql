{{
  config(
    dataset = 'stg_epex',
    materialized = 'view',
    tags = [
      'stg',
      'epex'
    ]
  )
}}

SELECT
  DATE AS date
  , HOUR AS hour
  , PRODUCT_TYPE AS product_type
  , PRICE AS price
FROM {{ ref('epex_avg_elec_2021-2025') }}
