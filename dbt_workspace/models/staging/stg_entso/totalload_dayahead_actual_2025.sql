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

SELECT
  REPLACE(SPLIT(Time__CET_CEST_, ' - ')[OFFSET(0)], '.', '-') AS start_datetime
  , REPLACE(SPLIT(Time__CET_CEST_, ' - ')[OFFSET(1)], '.', '-') AS end_datetime
  , CAST(NULLIF(Day_ahead_Total_Load_Forecast__MW____BZN_NL, '-') AS FLOAT64) AS day_ahead_total_load_forecast
  , CAST(NULLIF(Actual_Total_Load__MW____BZN_NL, '-') AS FLOAT64) AS actual_total_load
FROM {{ ref('entso-e_totalload_dayahead_actual_2025') }}
