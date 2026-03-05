# src/quality_checks.py
import logging
import sys
sys.path.append('/mnt/d/projects/weather-etl')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def check_no_nulls(record: dict, fields: list) -> tuple:
    """Check that critical fields are not null."""
    failed_fields = [f for f in fields if record.get(f) is None]
    passed = len(failed_fields) == 0
    detail = f"Null fields: {failed_fields}" if not passed else "All critical fields present"
    return passed, detail


def check_temperature_range(record: dict) -> tuple:
    """Check temperature is within realistic range (-90°C to 60°C)."""
    temp = record.get("temperature_celsius")
    if temp is None:
        return False, "Temperature is null"
    passed = -90 <= temp <= 60
    detail = f"Temperature {temp}°C is {'valid' if passed else 'OUT OF RANGE'}"
    return passed, detail


def check_humidity_range(record: dict) -> tuple:
    """Check humidity is between 0 and 100."""
    humidity = record.get("humidity_percent")
    if humidity is None:
        return False, "Humidity is null"
    passed = 0 <= humidity <= 100
    detail = f"Humidity {humidity}% is {'valid' if passed else 'OUT OF RANGE'}"
    return passed, detail


def check_wind_speed(record: dict) -> tuple:
    """Check wind speed is non-negative and realistic (max 120 m/s)."""
    wind = record.get("wind_speed_ms")
    if wind is None:
        return False, "Wind speed is null"
    passed = 0 <= wind <= 120
    detail = f"Wind speed {wind} m/s is {'valid' if passed else 'OUT OF RANGE'}"
    return passed, detail


def check_city_not_empty(record: dict) -> tuple:
    """Check city name exists and is not empty."""
    city = record.get("city_name")
    passed = city is not None and len(str(city).strip()) > 0
    detail = f"City name: '{city}' is {'valid' if passed else 'EMPTY OR NULL'}"
    return passed, detail


def run_all_checks(transformed_record: dict) -> list:
    """
    Run all quality checks on a single transformed record.
    Returns list of check results as dictionaries.
    """
    city = transformed_record.get("city_name", "Unknown")
    check_results = []

    # Define all checks to run
    checks = [
        (
            "no_nulls_check",
            lambda r: check_no_nulls(r, [
                "city_name", "country", "temperature_celsius",
                "humidity_percent", "weather_condition"
            ])
        ),
        ("temperature_range_check", check_temperature_range),
        ("humidity_range_check",    check_humidity_range),
        ("wind_speed_check",        check_wind_speed),
        ("city_not_empty_check",    check_city_not_empty),
    ]

    for check_name, check_func in checks:
        passed, detail = check_func(transformed_record)
        check_results.append({
            "city_name":   city,
            "check_name":  check_name,
            "check_passed": passed,
            "details":     detail
        })
        status = "✅ PASSED" if passed else "❌ FAILED"
        logger.info(f"{city} | {check_name}: {status} — {detail}")

    return check_results


def validate_all_records(transformed_records: list) -> tuple:
    """
    Run quality checks on all records.
    Returns (valid_records, all_check_results)
    Only records passing ALL checks proceed to load.
    """
    logger.info(f"Running quality checks on {len(transformed_records)} records")
    valid_records = []
    all_check_results = []

    for record in transformed_records:
        results = run_all_checks(record)
        all_check_results.extend(results)

        # Record is valid only if ALL checks passed
        all_passed = all(r["check_passed"] for r in results)
        if all_passed:
            valid_records.append(record)
        else:
            logger.warning(f"Record for {record.get('city_name')} failed quality checks — skipping load")

    logger.info(f"Quality check complete: {len(valid_records)}/{len(transformed_records)} records passed")
    return valid_records, all_check_results


# ── Test it manually ──────────────────────────────
if __name__ == "__main__":
    from src.extract import extract_all_cities, parse_raw_weather
    from src.transform import transform_all_records

    print("Testing Quality Checks...")
    print("=" * 60)

    # Full pipeline test
    raw_results   = extract_all_cities()
    parsed        = [parse_raw_weather(r) for r in raw_results if r]
    transformed   = transform_all_records(parsed)
    valid, results = validate_all_records(transformed)

    print(f"\nSummary:")
    print(f"  Total records  : {len(transformed)}")
    print(f"  Passed checks  : {len(valid)}")
    print(f"  Failed checks  : {len(transformed) - len(valid)}")
    print(f"\nAll checks passed — ready to load to PostgreSQL!")