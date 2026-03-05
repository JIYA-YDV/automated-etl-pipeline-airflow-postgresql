# src/load.py
import psycopg2
import logging
import sys
from datetime import datetime
sys.path.append('/mnt/d/projects/weather-etl')
from config.config import DB_CONFIG

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_db_connection():
    """Create and return a PostgreSQL connection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Database connection established")
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Database connection failed: {e}")
        return None


def load_raw_records(conn, raw_records: list) -> int:
    """
    Insert raw weather records into raw_weather table.
    Returns number of records inserted.
    """
    if not raw_records:
        logger.warning("No raw records to load")
        return 0

    sql = """
        INSERT INTO raw_weather (
            city_name, country, temperature_kelvin, feels_like_kelvin,
            humidity_percent, pressure_hpa, wind_speed_ms,
            weather_condition, weather_description,
            visibility_meters, timestamp_utc, collected_at
        ) VALUES (
            %(city_name)s, %(country)s, %(temperature_kelvin)s,
            %(feels_like_kelvin)s, %(humidity_percent)s, %(pressure_hpa)s,
            %(wind_speed_ms)s, %(weather_condition)s, %(weather_description)s,
            %(visibility_meters)s, %(timestamp_utc)s, %(collected_at)s
        )
    """

    try:
        cursor = conn.cursor()
        cursor.executemany(sql, raw_records)
        conn.commit()
        count = cursor.rowcount
        cursor.close()
        logger.info(f"Loaded {count} raw records into raw_weather table")
        return count
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"Failed to load raw records: {e}")
        return 0


def load_transformed_records(conn, transformed_records: list) -> int:
    """
    Insert transformed weather records into transformed_weather table.
    Returns number of records inserted.
    """
    if not transformed_records:
        logger.warning("No transformed records to load")
        return 0

    sql = """
        INSERT INTO transformed_weather (
            city_name, country, temperature_celsius, feels_like_celsius,
            humidity_percent, pressure_hpa, wind_speed_ms,
            weather_condition, weather_description, visibility_km,
            weather_category, recorded_at, collected_at
        ) VALUES (
            %(city_name)s, %(country)s, %(temperature_celsius)s,
            %(feels_like_celsius)s, %(humidity_percent)s, %(pressure_hpa)s,
            %(wind_speed_ms)s, %(weather_condition)s, %(weather_description)s,
            %(visibility_km)s, %(weather_category)s,
            %(recorded_at)s, %(collected_at)s
        )
    """

    try:
        cursor = conn.cursor()
        cursor.executemany(sql, transformed_records)
        conn.commit()
        count = cursor.rowcount
        cursor.close()
        logger.info(f"Loaded {count} transformed records into transformed_weather table")
        return count
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"Failed to load transformed records: {e}")
        return 0


def load_quality_logs(conn, check_results: list) -> int:
    """Insert quality check results into quality_logs table."""
    if not check_results:
        return 0

    sql = """
        INSERT INTO quality_logs (
            city_name, check_name, check_passed, details
        ) VALUES (
            %(city_name)s, %(check_name)s, %(check_passed)s, %(details)s
        )
    """

    try:
        cursor = conn.cursor()
        cursor.executemany(sql, check_results)
        conn.commit()
        count = cursor.rowcount
        cursor.close()
        logger.info(f"Logged {count} quality check results")
        return count
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"Failed to load quality logs: {e}")
        return 0


def log_pipeline_run(conn, status: str, extracted: int,
                     transformed: int, loaded: int, error: str = None):
    """Log the overall pipeline run result."""
    sql = """
        INSERT INTO pipeline_logs (
            status, records_extracted, records_transformed,
            records_loaded, error_message
        ) VALUES (%s, %s, %s, %s, %s)
    """
    try:
        cursor = conn.cursor()
        cursor.execute(sql, (status, extracted, transformed, loaded, error))
        conn.commit()
        cursor.close()
        logger.info(f"Pipeline run logged: status={status}, loaded={loaded} records")
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"Failed to log pipeline run: {e}")


# ── Test it manually ──────────────────────────────
if __name__ == "__main__":
    from src.extract import extract_all_cities, parse_raw_weather
    from src.transform import transform_all_records
    from src.quality_checks import validate_all_records

    print("Testing Full ETL Pipeline...")
    print("=" * 60)

    # Step 1: Extract
    raw_results  = extract_all_cities()
    parsed       = [parse_raw_weather(r) for r in raw_results if r]
    print(f"Extracted: {len(parsed)} records")

    # Step 2: Transform
    transformed  = transform_all_records(parsed)
    print(f"Transformed: {len(transformed)} records")

    # Step 3: Quality checks
    valid, check_results = validate_all_records(transformed)
    print(f"Passed quality checks: {len(valid)} records")

    # Step 4: Load to PostgreSQL
    conn = get_db_connection()
    if conn:
        raw_count         = load_raw_records(conn, parsed)
        transformed_count = load_transformed_records(conn, valid)
        load_quality_logs(conn, check_results)
        log_pipeline_run(
            conn,
            status="SUCCESS",
            extracted=len(parsed),
            transformed=len(transformed),
            loaded=transformed_count
        )
        conn.close()

        print("\n" + "=" * 60)
        print("PIPELINE COMPLETE!")
        print(f"  Extracted  : {len(parsed)} records")
        print(f"  Transformed: {len(transformed)} records")
        print(f"  Loaded     : {transformed_count} records to PostgreSQL")
        print("\nCheck pgAdmin → weather_pipeline → transformed_weather table!")
    else:
        print("Database connection failed — check your .env file")