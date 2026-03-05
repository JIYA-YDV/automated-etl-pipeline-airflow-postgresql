# src/transform.py
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def kelvin_to_celsius(kelvin: float) -> float:
    """Convert Kelvin to Celsius."""
    if kelvin is None:
        return None
    return round(kelvin - 273.15, 2)


def meters_to_km(meters: int) -> float:
    """Convert visibility from meters to kilometers."""
    if meters is None:
        return None
    return round(meters / 1000, 2)


def categorize_weather(condition: str) -> str:
    """
    Categorize weather into simplified buckets for analysis.
    """
    if condition is None:
        return "Unknown"

    condition_lower = condition.lower()

    if condition_lower in ["clear"]:
        return "Clear"
    elif condition_lower in ["clouds"]:
        return "Cloudy"
    elif condition_lower in ["rain", "drizzle"]:
        return "Rainy"
    elif condition_lower in ["snow"]:
        return "Snowy"
    elif condition_lower in ["thunderstorm"]:
        return "Stormy"
    elif condition_lower in ["mist", "fog", "haze", "smoke", "dust"]:
        return "Foggy"
    else:
        return "Other"


def transform_weather_record(raw_record: dict) -> dict:
    """
    Transform a single raw weather record into clean format.
    - Converts Kelvin to Celsius
    - Converts meters to km
    - Adds weather category
    - Converts Unix timestamp to readable datetime
    """
    try:
        transformed = {
            "city_name": raw_record["city_name"],
            "country": raw_record["country"],
            "temperature_celsius": kelvin_to_celsius(
                raw_record["temperature_kelvin"]
            ),
            "feels_like_celsius": kelvin_to_celsius(
                raw_record["feels_like_kelvin"]
            ),
            "humidity_percent": raw_record["humidity_percent"],
            "pressure_hpa": raw_record["pressure_hpa"],
            "wind_speed_ms": raw_record["wind_speed_ms"],
            "weather_condition": raw_record["weather_condition"],
            "weather_description": raw_record["weather_description"],
            "visibility_km": meters_to_km(raw_record["visibility_meters"]),
            "weather_category": categorize_weather(
                raw_record["weather_condition"]
            ),
            "recorded_at": datetime.utcfromtimestamp(
                raw_record["timestamp_utc"]
            ),
            "collected_at": raw_record["collected_at"]
        }

        logger.info(f"Transformed record for {transformed['city_name']}: "
                   f"{transformed['temperature_celsius']}°C, "
                   f"{transformed['weather_category']}")
        return transformed

    except KeyError as e:
        logger.error(f"Missing field during transform: {e}")
        return None


def transform_all_records(raw_records: list) -> list:
    """
    Transform all raw weather records.
    Returns list of transformed records, skipping any failures.
    """
    logger.info(f"Starting transformation of {len(raw_records)} records")
    transformed = []

    for record in raw_records:
        result = transform_weather_record(record)
        if result:
            transformed.append(result)

    logger.info(f"Transformation complete: {len(transformed)}/{len(raw_records)} records successful")
    return transformed


# ── Test it manually ──────────────────────────────
if __name__ == "__main__":
    import sys
    sys.path.append('/mnt/d/projects/weather-etl')
    from src.extract import extract_all_cities, parse_raw_weather

    print("Testing full Extract + Transform...")
    print("=" * 50)

    # Extract
    raw_results = extract_all_cities()
    parsed_records = [parse_raw_weather(r) for r in raw_results if r]

    # Transform
    transformed = transform_all_records(parsed_records)

    # Display results
    print(f"\nTransformed {len(transformed)} records:")
    print("=" * 50)
    for record in transformed:
        print(f"\n{record['city_name']}, {record['country']}")
        print(f"  Temperature : {record['temperature_celsius']}°C "
              f"(feels like {record['feels_like_celsius']}°C)")
        print(f"  Humidity    : {record['humidity_percent']}%")
        print(f"  Wind Speed  : {record['wind_speed_ms']} m/s")
        print(f"  Condition   : {record['weather_description']}")
        print(f"  Category    : {record['weather_category']}")
        print(f"  Visibility  : {record['visibility_km']} km")
        print(f"  Recorded at : {record['recorded_at']}")