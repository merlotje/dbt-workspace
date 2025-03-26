WITH energy_prices_per_day AS (
  SELECT
    date
    , SUM(forecast_load_kwh) AS forecast_load
    , SUM(actual_load_kwh) AS actual_load
    , SUM(forecast_cost) AS forecast_cost
    , SUM(actual_cost) AS actual_cost
  FROM {{ ref('electricity_prices_per_load') }}
  GROUP BY date
)

SELECT
  date
  , forecast_load
  , actual_load
  , {{ calculate_difference('forecast_load_kwh', 'actual_load_kwh') }} AS deviation_forecast_actual_load
  , forecast_cost
  , actual_cost
  , {{ calculate_difference('forecast_cost', 'actual_cost') }} AS deviation_forecast_actual_cost
FROM energy_prices_per_day
ORDER BY ABS({{ calculate_difference('forecast_load_kwh', 'actual_load_kwh') }}) DESC
LIMIT 10