# src/extract.py
import requests
import logging
from datetime import datetime
import sys
sys.path.append('/mnt/d/projects/weather-etl')
from config.config import OPENWEATHER_API_KEY, BASE_URL, CITIES

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def fetch_weather_for_city(city_name: str) -> dict:
    """
    Fetch raw weather data for a single city from OpenWeatherMap API.
    Returns raw JSON response as dictionary.
    """
    params = {
        "q": city_name,
        "appid": OPENWEATHER_API_KEY,
        "units": "standard"  # Kelvin — we convert later in transform
    }

    try:
        logger.info(f"Fetching weather data for: {city_name}")
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()  # Raises error if status != 200

        data = response.json()
        logger.info(f"Successfully fetched data for {city_name}")
        return data

    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error for {city_name}: {e}")
        return None
    except requests.exceptions.ConnectionError:
        logger.error(f"Connection error for {city_name} - check internet")
        return None
    except requests.exceptions.Timeout:
        logger.error(f"Timeout for {city_name}")
        return None


def extract_all_cities() -> list:
    """
    Fetch weather data for all cities defined in config.
    Returns list of raw API responses.
    """
    logger.info("Starting extraction for all cities")
    results = []

    for city in CITIES:
        data = fetch_weather_for_city(city)
        if data:
            results.append(data)

    logger.info(f"Extraction complete. Got data for {len(results)}/{len(CITIES)} cities")
    return results


def parse_raw_weather(raw_data: dict) -> dict:
    """
    Parse raw API JSON into a flat dictionary matching our raw_weather table.
    No transformations here — just extracting the fields we need.
    """
    try:
        return {
            "city_name": raw_data["name"],
            "country": raw_data["sys"]["country"],
            "temperature_kelvin": raw_data["main"]["temp"],
            "feels_like_kelvin": raw_data["main"]["feels_like"],
            "humidity_percent": raw_data["main"]["humidity"],
            "pressure_hpa": raw_data["main"]["pressure"],
            "wind_speed_ms": raw_data["wind"]["speed"],
            "weather_condition": raw_data["weather"][0]["main"],
            "weather_description": raw_data["weather"][0]["description"],
            "visibility_meters": raw_data.get("visibility", None),
            "timestamp_utc": raw_data["dt"],
            "collected_at": datetime.utcnow()
        }
    except KeyError as e:
        logger.error(f"Missing field in API response: {e}")
        return None


# ── Test it manually ──────────────────────────────
if __name__ == "__main__":
    print("Testing extraction...")
    raw_results = extract_all_cities()

    print(f"\nGot {len(raw_results)} results")
    print("\nParsing first result:")
    if raw_results:
        parsed = parse_raw_weather(raw_results[0])
        for key, value in parsed.items():
            print(f"  {key}: {value}")