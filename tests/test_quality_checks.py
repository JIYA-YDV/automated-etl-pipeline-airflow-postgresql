# tests/test_quality_checks.py
import sys
import pytest
sys.path.append('/mnt/d/projects/weather-etl')

from src.quality_checks import (
    check_no_nulls,
    check_temperature_range,
    check_humidity_range,
    check_wind_speed,
    check_city_not_empty
)


# ── check_no_nulls tests ──────────────────────────────
def test_no_nulls_all_present():
    record = {"city_name": "Kathmandu", "temperature_celsius": 13.54}
    passed, detail = check_no_nulls(record, ["city_name", "temperature_celsius"])
    assert passed is True

def test_no_nulls_missing_field():
    record = {"city_name": "Kathmandu", "temperature_celsius": None}
    passed, detail = check_no_nulls(record, ["city_name", "temperature_celsius"])
    assert passed is False


# ── check_temperature_range tests ────────────────────
def test_temperature_valid():
    record = {"temperature_celsius": 13.54}
    passed, _ = check_temperature_range(record)
    assert passed is True

def test_temperature_too_hot():
    record = {"temperature_celsius": 100.0}
    passed, _ = check_temperature_range(record)
    assert passed is False

def test_temperature_too_cold():
    record = {"temperature_celsius": -100.0}
    passed, _ = check_temperature_range(record)
    assert passed is False

def test_temperature_none():
    record = {"temperature_celsius": None}
    passed, _ = check_temperature_range(record)
    assert passed is False


# ── check_humidity_range tests ────────────────────────
def test_humidity_valid():
    record = {"humidity_percent": 49}
    passed, _ = check_humidity_range(record)
    assert passed is True

def test_humidity_over_100():
    record = {"humidity_percent": 150}
    passed, _ = check_humidity_range(record)
    assert passed is False

def test_humidity_negative():
    record = {"humidity_percent": -5}
    passed, _ = check_humidity_range(record)
    assert passed is False


# ── check_wind_speed tests ────────────────────────────
def test_wind_speed_valid():
    record = {"wind_speed_ms": 5.0}
    passed, _ = check_wind_speed(record)
    assert passed is True

def test_wind_speed_negative():
    record = {"wind_speed_ms": -1.0}
    passed, _ = check_wind_speed(record)
    assert passed is False

def test_wind_speed_extreme():
    record = {"wind_speed_ms": 200.0}
    passed, _ = check_wind_speed(record)
    assert passed is False


# ── check_city_not_empty tests ────────────────────────
def test_city_valid():
    record = {"city_name": "Kathmandu"}
    passed, _ = check_city_not_empty(record)
    assert passed is True

def test_city_empty_string():
    record = {"city_name": ""}
    passed, _ = check_city_not_empty(record)
    assert passed is False

def test_city_none():
    record = {"city_name": None}
    passed, _ = check_city_not_empty(record)
    assert passed is False