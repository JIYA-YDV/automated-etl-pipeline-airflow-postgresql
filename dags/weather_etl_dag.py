# dags/weather_etl_dag.py
import sys
import logging
from datetime import datetime, timedelta
sys.path.append('/mnt/d/projects/weather-etl')

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.extract import extract_all_cities, parse_raw_weather
from src.transform import transform_all_records
from src.quality_checks import validate_all_records
from src.load import (
    get_db_connection,
    load_raw_records,
    load_transformed_records,
    load_quality_logs,
    log_pipeline_run
)

logger = logging.getLogger(__name__)

# ── Default arguments for all tasks ──────────────────
default_args = {
    "owner": "jiya_yadav",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG Definition ────────────────────────────────────
dag = DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    description="Hourly ETL pipeline: OpenWeatherMap API → PostgreSQL",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["weather", "etl", "postgresql"]
)

# ── Task Functions ────────────────────────────────────

def task_extract(**context):
    """Extract weather data from API and push to XCom."""
    logger.info("Starting extraction task")
    raw_results = extract_all_cities()
    parsed = [parse_raw_weather(r) for r in raw_results if r]

    if not parsed:
        raise ValueError("Extraction failed — no data returned from API")

    # Push to XCom so next task can access it
    context["ti"].xcom_push(key="parsed_records", value=parsed)
    logger.info(f"Extraction complete: {len(parsed)} records pushed to XCom")
    return len(parsed)


def task_transform(**context):
    """Transform raw records and push to XCom."""
    logger.info("Starting transformation task")

    # Pull from previous task via XCom
    parsed = context["ti"].xcom_pull(
        key="parsed_records",
        task_ids="extract_weather"
    )

    if not parsed:
        raise ValueError("No parsed records received from extract task")

    transformed = transform_all_records(parsed)

    if not transformed:
        raise ValueError("Transformation produced no records")

    context["ti"].xcom_push(key="transformed_records", value=transformed)
    logger.info(f"Transformation complete: {len(transformed)} records")
    return len(transformed)


def task_quality_check(**context):
    """Run quality checks and push valid records to XCom."""
    logger.info("Starting quality check task")

    transformed = context["ti"].xcom_pull(
        key="transformed_records",
        task_ids="transform_weather"
    )

    if not transformed:
        raise ValueError("No transformed records received")

    valid, check_results = validate_all_records(transformed)

    if not valid:
        raise ValueError("All records failed quality checks — stopping pipeline")

    context["ti"].xcom_push(key="valid_records", value=valid)
    context["ti"].xcom_push(key="check_results", value=check_results)
    logger.info(f"Quality checks complete: {len(valid)}/{len(transformed)} passed")
    return len(valid)


def task_load(**context):
    """Load validated records into PostgreSQL."""
    logger.info("Starting load task")

    # Pull all data from previous tasks
    parsed = context["ti"].xcom_pull(
        key="parsed_records",
        task_ids="extract_weather"
    )
    valid = context["ti"].xcom_pull(
        key="valid_records",
        task_ids="quality_check_weather"
    )
    check_results = context["ti"].xcom_pull(
        key="check_results",
        task_ids="quality_check_weather"
    )

    conn = get_db_connection()
    if not conn:
        raise ConnectionError("Could not connect to PostgreSQL")

    try:
        raw_count         = load_raw_records(conn, parsed)
        transformed_count = load_transformed_records(conn, valid)
        load_quality_logs(conn, check_results)
        log_pipeline_run(
            conn,
            status="SUCCESS",
            extracted=len(parsed),
            transformed=len(valid),
            loaded=transformed_count
        )
        logger.info(f"Load complete: {transformed_count} records loaded")
        return transformed_count
    except Exception as e:
        log_pipeline_run(
            conn,
            status="FAILED",
            extracted=len(parsed) if parsed else 0,
            transformed=len(valid) if valid else 0,
            loaded=0,
            error=str(e)
        )
        raise
    finally:
        conn.close()


# ── Task Definitions ──────────────────────────────────
extract_task = PythonOperator(
    task_id="extract_weather",
    python_callable=task_extract,
    dag=dag
)

transform_task = PythonOperator(
    task_id="transform_weather",
    python_callable=task_transform,
    dag=dag
)

quality_task = PythonOperator(
    task_id="quality_check_weather",
    python_callable=task_quality_check,
    dag=dag
)

load_task = PythonOperator(
    task_id="load_to_postgres",
    python_callable=task_load,
    dag=dag
)

# ── Pipeline Order ────────────────────────────────────
extract_task >> transform_task >> quality_task >> load_task