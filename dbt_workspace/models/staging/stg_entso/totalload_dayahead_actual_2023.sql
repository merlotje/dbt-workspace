{{
  config(
    dataset = 'stg_entso',
    materialized = 'view',
    tags = [
      'stg',
      'entso_e'
    ]
  )
}}

WITH cleansed AS (
  SELECT
    REPLACE(SPLIT(Time__CET_CEST_, ' - ')[OFFSET(0)], '.', '-') AS start_datetime
    , REPLACE(SPLIT(Time__CET_CEST_, ' - ')[OFFSET(1)], '.', '-') AS end_datetime
    , CAST(NULLIF(Day_ahead_Total_Load_Forecast__MW____BZN_NL, '-') AS FLOAT64) AS day_ahead_total_load_forecast
    , CAST(NULLIF(Actual_Total_Load__MW____BZN_NL, '-') AS FLOAT64) AS actual_total_load
  FROM {{ ref('entso-e_totalload_dayahead_actual_2023') }}
)

SELECT
  REGEXP_EXTRACT(start_datetime, r'(\d{2}-\d{2}-\d{4})') AS start_date
  , REGEXP_EXTRACT(end_datetime, r'(\d{2}-\d{2}-\d{4})') AS end_date
  , CAST(REGEXP_EXTRACT(SPLIT(start_datetime, ' ')[OFFSET(1)], r'^(\d{2})') AS INT64) AS start_hour
  , CAST(REGEXP_EXTRACT(SPLIT(end_datetime, ' ')[OFFSET(1)], r'^(\d{2})') AS INT64) AS end_hour
  , day_ahead_total_load_forecast
  , actual_total_load
FROM cleansed
