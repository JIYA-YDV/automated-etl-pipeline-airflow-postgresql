SET search_path TO public;

CREATE TABLE IF NOT EXISTS raw_weather (
    id                  SERIAL PRIMARY KEY,
    city_name           VARCHAR(100) NOT NULL,
    country             VARCHAR(10) NOT NULL,
    temperature_kelvin  NUMERIC(10,2),
    feels_like_kelvin   NUMERIC(10,2),
    humidity_percent    INTEGER,
    pressure_hpa        NUMERIC(10,2),
    wind_speed_ms       NUMERIC(10,2),
    weather_condition   VARCHAR(100),
    weather_description VARCHAR(200),
    visibility_meters   INTEGER,
    timestamp_utc       BIGINT,
    collected_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transformed_weather (
    id                      SERIAL PRIMARY KEY,
    city_name               VARCHAR(100) NOT NULL,
    country                 VARCHAR(10) NOT NULL,
    temperature_celsius     NUMERIC(10,2),
    feels_like_celsius      NUMERIC(10,2),
    humidity_percent        INTEGER,
    pressure_hpa            NUMERIC(10,2),
    wind_speed_ms           NUMERIC(10,2),
    weather_condition       VARCHAR(100),
    weather_description     VARCHAR(200),
    visibility_km           NUMERIC(10,2),
    weather_category        VARCHAR(50),
    recorded_at             TIMESTAMP,
    collected_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS quality_logs (
    id              SERIAL PRIMARY KEY,
    run_timestamp   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    city_name       VARCHAR(100),
    check_name      VARCHAR(200),
    check_passed    BOOLEAN,
    details         TEXT
);

CREATE TABLE IF NOT EXISTS pipeline_logs (
    id                  SERIAL PRIMARY KEY,
    run_timestamp       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status              VARCHAR(50),
    records_extracted   INTEGER,
    records_transformed INTEGER,
    records_loaded      INTEGER,
    error_message       TEXT
);