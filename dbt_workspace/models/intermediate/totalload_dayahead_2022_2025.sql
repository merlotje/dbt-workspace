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
  REGEXP_EXTRACT(start_datetime, r'(\d{2}-\d{2}-\d{4})') AS start_date
  , REGEXP_EXTRACT(end_datetime, r'(\d{2}-\d{2}-\d{4})') AS end_date
  , CAST(REGEXP_EXTRACT(SPLIT(start_datetime, ' ')[OFFSET(1)], r'^(\d{2})') AS INT64) AS start_hour
  , CAST(REGEXP_EXTRACT(SPLIT(end_datetime, ' ')[OFFSET(1)], r'^(\d{2})') AS INT64) AS end_hour
  , day_ahead_total_load_forecast
  , actual_total_load
FROM {{ ref('totalload_dayahead_actual_2022') }}

UNION ALL

SELECT
  REGEXP_EXTRACT(start_datetime, r'(\d{2}-\d{2}-\d{4})') AS start_date
  , REGEXP_EXTRACT(end_datetime, r'(\d{2}-\d{2}-\d{4})') AS end_date
  , CAST(REGEXP_EXTRACT(SPLIT(start_datetime, ' ')[OFFSET(1)], r'^(\d{2})') AS INT64) AS start_hour
  , CAST(REGEXP_EXTRACT(SPLIT(end_datetime, ' ')[OFFSET(1)], r'^(\d{2})') AS INT64) AS end_hour
  , day_ahead_total_load_forecast
  , actual_total_load
FROM {{ ref('totalload_dayahead_actual_2023') }}

UNION ALL

SELECT
  REGEXP_EXTRACT(start_datetime, r'(\d{2}-\d{2}-\d{4})') AS start_date
  , REGEXP_EXTRACT(end_datetime, r'(\d{2}-\d{2}-\d{4})') AS end_date
  , CAST(REGEXP_EXTRACT(SPLIT(start_datetime, ' ')[OFFSET(1)], r'^(\d{2})') AS INT64) AS start_hour
  , CAST(REGEXP_EXTRACT(SPLIT(end_datetime, ' ')[OFFSET(1)], r'^(\d{2})') AS INT64) AS end_hour
  , day_ahead_total_load_forecast
  , actual_total_load
FROM {{ ref('totalload_dayahead_actual_2024') }}

UNION ALL

SELECT
  REGEXP_EXTRACT(start_datetime, r'(\d{2}-\d{2}-\d{4})') AS start_date
  , REGEXP_EXTRACT(end_datetime, r'(\d{2}-\d{2}-\d{4})') AS end_date
  , CAST(REGEXP_EXTRACT(SPLIT(start_datetime, ' ')[OFFSET(1)], r'^(\d{2})') AS INT64) AS start_hour
  , CAST(REGEXP_EXTRACT(SPLIT(end_datetime, ' ')[OFFSET(1)], r'^(\d{2})') AS INT64) AS end_hour
  , day_ahead_total_load_forecast
  , actual_total_load
FROM {{ ref('totalload_dayahead_actual_2025') }}
