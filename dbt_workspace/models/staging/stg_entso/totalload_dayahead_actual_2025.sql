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
  , {{ to_null('Day_ahead_Total_Load_Forecast__MW____BZN_NL') }} AS day_ahead_total_load_forecast
  , {{ to_null('Actual_Total_Load__MW____BZN_NL') }} AS actual_total_load
FROM {{ ref('entso-e_totalload_dayahead_actual_2025') }}
