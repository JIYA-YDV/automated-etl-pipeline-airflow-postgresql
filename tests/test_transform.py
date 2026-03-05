# tests/test_transform.py
import sys
import pytest
sys.path.append('/mnt/d/projects/weather-etl')

from src.transform import (
    kelvin_to_celsius,
    meters_to_km,
    categorize_weather,
    transform_weather_record
)


# ── kelvin_to_celsius tests ───────────────────────────
def test_kelvin_to_celsius_freezing():
    """273.15K should equal exactly 0°C."""
    assert kelvin_to_celsius(273.15) == 0.0

def test_kelvin_to_celsius_boiling():
    """373.15K should equal exactly 100°C."""
    assert kelvin_to_celsius(373.15) == 100.0

def test_kelvin_to_celsius_kathmandu():
    """286.69K should equal 13.54°C."""
    assert kelvin_to_celsius(286.69) == 13.54

def test_kelvin_to_celsius_none():
    """None input should return None."""
    assert kelvin_to_celsius(None) is None


# ── meters_to_km tests ────────────────────────────────
def test_meters_to_km_standard():
    """10000 meters should equal 10.0 km."""
    assert meters_to_km(10000) == 10.0

def test_meters_to_km_none():
    """None input should return None."""
    assert meters_to_km(None) is None

def test_meters_to_km_zero():
    """0 meters should equal 0.0 km."""
    assert meters_to_km(0) == 0.0


# ── categorize_weather tests ──────────────────────────
def test_categorize_clear():
    assert categorize_weather("Clear") == "Clear"

def test_categorize_clouds():
    assert categorize_weather("Clouds") == "Cloudy"

def test_categorize_rain():
    assert categorize_weather("Rain") == "Rainy"

def test_categorize_snow():
    assert categorize_weather("Snow") == "Snowy"

def test_categorize_thunderstorm():
    assert categorize_weather("Thunderstorm") == "Stormy"

def test_categorize_mist():
    assert categorize_weather("Mist") == "Foggy"

def test_categorize_none():
    assert categorize_weather(None) == "Unknown"

def test_categorize_unknown():
    assert categorize_weather("Tornado") == "Other"


# ── transform_weather_record tests ───────────────────
def test_transform_complete_record():
    """Test full transformation of a valid record."""
    raw = {
        "city_name": "Kathmandu",
        "country": "NP",
        "temperature_kelvin": 286.69,
        "feels_like_kelvin": 285.38,
        "humidity_percent": 49,
        "pressure_hpa": 1014,
        "wind_speed_ms": 1.1,
        "weather_condition": "Clear",
        "weather_description": "clear sky",
        "visibility_meters": 10000,
        "timestamp_utc": 1772724673,
        "collected_at": "2026-03-05 15:34:30"
    }
    result = transform_weather_record(raw)

    assert result is not None
    assert result["city_name"] == "Kathmandu"
    assert result["temperature_celsius"] == 13.54
    assert result["visibility_km"] == 10.0
    assert result["weather_category"] == "Clear"

def test_transform_missing_field():
    """Record missing required field should return None."""
    bad_record = {
        "city_name": "Kathmandu",
        # missing all other fields
    }
    result = transform_weather_record(bad_record)
    assert result is None