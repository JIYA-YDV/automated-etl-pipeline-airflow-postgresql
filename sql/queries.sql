-- ================================================
-- queries.sql — Sample Analytical Queries
-- weather_pipeline database
-- ================================================

-- 1. Latest weather for all cities
SELECT 
    city_name,
    country,
    temperature_celsius,
    feels_like_celsius,
    humidity_percent,
    weather_category,
    wind_speed_ms,
    recorded_at
FROM transformed_weather
WHERE recorded_at = (
    SELECT MAX(recorded_at) 
    FROM transformed_weather t2 
    WHERE t2.city_name = transformed_weather.city_name
)
ORDER BY city_name;

-- 2. Average temperature per city (all time)
SELECT 
    city_name,
    ROUND(AVG(temperature_celsius)::numeric, 2) AS avg_temp_celsius,
    ROUND(MIN(temperature_celsius)::numeric, 2) AS min_temp_celsius,
    ROUND(MAX(temperature_celsius)::numeric, 2) AS max_temp_celsius,
    COUNT(*) AS total_records
FROM transformed_weather
GROUP BY city_name
ORDER BY avg_temp_celsius DESC;

-- 3. Weather category distribution per city
SELECT 
    city_name,
    weather_category,
    COUNT(*) AS occurrences,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY city_name), 1) AS percentage
FROM transformed_weather
GROUP BY city_name, weather_category
ORDER BY city_name, occurrences DESC;

-- 4. Pipeline run history
SELECT 
    run_timestamp,
    status,
    records_extracted,
    records_transformed,
    records_loaded,
    error_message
FROM pipeline_logs
ORDER BY run_timestamp DESC
LIMIT 10;

-- 5. Data quality check summary
SELECT 
    check_name,
    COUNT(*) AS total_checks,
    SUM(CASE WHEN check_passed THEN 1 ELSE 0 END) AS passed,
    SUM(CASE WHEN NOT check_passed THEN 1 ELSE 0 END) AS failed,
    ROUND(SUM(CASE WHEN check_passed THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pass_rate
FROM quality_logs
GROUP BY check_name
ORDER BY check_name;

-- 6. Hourly temperature trend for Kathmandu
SELECT 
    DATE_TRUNC('hour', recorded_at) AS hour,
    ROUND(AVG(temperature_celsius)::numeric, 2) AS avg_temp,
    ROUND(AVG(humidity_percent)::numeric, 1) AS avg_humidity
FROM transformed_weather
WHERE city_name = 'Kathmandu'
GROUP BY DATE_TRUNC('hour', recorded_at)
ORDER BY hour DESC;