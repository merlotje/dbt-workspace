{{
  config(
    dataset = 'energy_market',
    materialized = 'table',
    tags = [
      'marts',
    ]
  )
}}

SELECT
  entsoe.start_date AS date
  , entsoe.start_hour AS hour
  , (entsoe.forecast_total_load * 1000 * 24) AS forecast_load_kwh
  , (entsoe.actual_total_load * 1000 * 24) AS actual_load_kwh
  , (entsoe.forecast_total_load * 1000 * 24) * epex.price AS forecast_cost
  , (entsoe.actual_total_load * 1000 * 24) * epex.price AS actual_cost
FROM {{ ref('totalload_dayahead_2022_2025') }} AS entsoe
LEFT JOIN {{ ref('avg_elec_2021_2025') }} AS epex
  ON entsoe.start_date = epex.date
  AND entsoe.start_hour = epex.hour