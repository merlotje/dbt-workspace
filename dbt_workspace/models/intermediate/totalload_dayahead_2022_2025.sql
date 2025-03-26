{{
  config(
    dataset = 'itm_entso',
    materialized = 'table',
    tags = [
      'intermediate',
      'entso_e'
    ]
  )
}}

SELECT
  {{ strip_datetime('start_datetime') }} AS start_date
  , CAST(REGEXP_EXTRACT(SPLIT(start_datetime, ' ')[OFFSET(1)], r'^(\d{2})') AS INT64) AS start_hour
  , SUM(day_ahead_total_load_forecast) AS forecast_total_load
  , SUM(actual_total_load) AS actual_total_load
FROM {{ ref('totalload_dayahead_actual_2022') }}
GROUP BY ALL

UNION ALL

SELECT
  {{ strip_datetime('start_datetime') }} AS start_date
  , CAST(REGEXP_EXTRACT(SPLIT(start_datetime, ' ')[OFFSET(1)], r'^(\d{2})') AS INT64) AS start_hour
  , SUM(day_ahead_total_load_forecast) AS forecast_total_load
  , SUM(actual_total_load) AS actual_total_load
FROM {{ ref('totalload_dayahead_actual_2023') }}
GROUP BY ALL

UNION ALL

SELECT
  {{ strip_datetime('start_datetime') }} AS start_date
  , CAST(REGEXP_EXTRACT(SPLIT(start_datetime, ' ')[OFFSET(1)], r'^(\d{2})') AS INT64) AS start_hour
  , SUM(day_ahead_total_load_forecast) AS forecast_total_load
  , SUM(actual_total_load) AS actual_total_load
FROM {{ ref('totalload_dayahead_actual_2024') }}
GROUP BY ALL

UNION ALL

SELECT
  {{ strip_datetime('start_datetime') }} AS start_date
  , CAST(REGEXP_EXTRACT(SPLIT(start_datetime, ' ')[OFFSET(1)], r'^(\d{2})') AS INT64) AS start_hour
  , SUM(day_ahead_total_load_forecast) AS forecast_total_load
  , SUM(actual_total_load) AS actual_total_load
FROM {{ ref('totalload_dayahead_actual_2025') }}
GROUP BY ALL
