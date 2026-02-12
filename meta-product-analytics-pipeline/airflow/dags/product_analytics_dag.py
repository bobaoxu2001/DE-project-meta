"""
Airflow DAG — Product Analytics ETL Pipeline
=============================================
Orchestrates the daily ETL pipeline for product analytics:

    generate_data → extract → transform → load → quality_check → build_aggregates → notify

Schedule: Daily at 06:00 UTC
Retry: 3 attempts with 5-minute delay
SLA: Must complete within 2 hours

This DAG demonstrates production-grade pipeline orchestration with:
  - Idempotent tasks (safe to re-run)
  - Data quality gates (pipeline halts on critical failures)
  - Incremental processing (date-partitioned)
  - Alerting on failure
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule


# ---------------------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------------------

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-alerts@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def _extract_data(**context):
    """Extract raw data from the data lake."""
    from src.etl.extract import Extractor

    ds = context["ds"]  # Execution date (YYYY-MM-DD)
    extractor = Extractor(raw_data_dir="data/raw")

    # Extract events for the execution date
    events_df = extractor.extract_events_for_date(ds)
    users_df = extractor.extract_users()

    # Push to XCom for downstream tasks
    context["ti"].xcom_push(key="events_count", value=len(events_df))
    context["ti"].xcom_push(key="users_count", value=len(users_df))

    return {"events": len(events_df), "users": len(users_df)}


def _transform_data(**context):
    """Clean and transform raw data."""
    from src.etl.extract import Extractor
    from src.etl.transform import Transformer

    ds = context["ds"]
    extractor = Extractor(raw_data_dir="data/raw")
    transformer = Transformer()

    # Extract
    events_df = extractor.extract_events_for_date(ds)
    users_df = extractor.extract_users()

    # Transform
    clean_events = transformer.clean_events(events_df)
    user_dim = transformer.build_user_dimension(users_df)
    fact_events = transformer.build_fact_events(clean_events, user_dim)

    context["ti"].xcom_push(key="fact_count", value=len(fact_events))
    return {"facts": len(fact_events)}


def _load_data(**context):
    """Load transformed data into the warehouse."""
    from src.etl.extract import Extractor
    from src.etl.load import Loader
    from src.etl.transform import Transformer
    from src.models.schema import WarehouseSchema

    ds = context["ds"]

    # Full ETL for the date
    extractor = Extractor(raw_data_dir="data/raw")
    transformer = Transformer()

    events_df = extractor.extract_events_for_date(ds)
    users_df = extractor.extract_users()

    clean_events = transformer.clean_events(events_df)
    user_dim = transformer.build_user_dimension(users_df)
    fact_events = transformer.build_fact_events(clean_events, user_dim)

    # Load
    warehouse = WarehouseSchema()
    loader = Loader(warehouse.conn)
    loader.load_facts(fact_events, partition_date=ds)

    warehouse.close()
    return {"loaded": len(fact_events)}


def _run_quality_checks(**context):
    """Run data quality checks and decide if pipeline should continue."""
    from src.data_quality.checks import DataQualityChecker
    from src.models.schema import WarehouseSchema

    warehouse = WarehouseSchema()
    checker = DataQualityChecker(warehouse.conn)
    results = checker.run_all_checks()
    warehouse.close()

    # Push results to XCom
    context["ti"].xcom_push(key="dq_results", value=results)

    return results


def _check_quality_gate(**context):
    """Branch: continue if quality checks pass, else alert."""
    ti = context["ti"]
    results = ti.xcom_pull(task_ids="quality_check", key="dq_results")

    if results and results.get("failed", 0) == 0:
        return "build_aggregates"
    else:
        return "quality_alert"


def _build_aggregates(**context):
    """Compute aggregate tables."""
    from src.etl.transform import Transformer
    from src.etl.load import Loader
    from src.models.schema import WarehouseSchema

    ds = context["ds"]
    warehouse = WarehouseSchema()
    loader = Loader(warehouse.conn)

    # Fetch fact data for the date
    fact_df = warehouse.conn.execute(f"""
        SELECT * FROM analytics.fct_events
        WHERE _partition_date = DATE '{ds}'
    """).fetchdf()

    user_dim = warehouse.conn.execute(
        "SELECT * FROM analytics.dim_users"
    ).fetchdf()

    if not fact_df.empty:
        transformer = Transformer()
        daily_agg = transformer.compute_daily_aggregates(fact_df, user_dim)
        loader.load_aggregates(daily_agg, "analytics.agg_daily_metrics")

    warehouse.close()
    return {"aggregates_built": True}


def _send_notification(**context):
    """Send pipeline completion notification."""
    ds = context["ds"]
    ti = context["ti"]

    events_count = ti.xcom_pull(task_ids="extract", key="events_count") or 0
    fact_count = ti.xcom_pull(task_ids="transform", key="fact_count") or 0

    # In production, this would send to Slack / email / PagerDuty
    message = (
        f"Product Analytics Pipeline Complete\n"
        f"Date: {ds}\n"
        f"Events extracted: {events_count}\n"
        f"Facts loaded: {fact_count}\n"
        f"Status: SUCCESS"
    )
    print(message)
    return message


def _quality_alert(**context):
    """Alert on data quality failures."""
    ti = context["ti"]
    results = ti.xcom_pull(task_ids="quality_check", key="dq_results")

    message = (
        f"DATA QUALITY ALERT\n"
        f"Pipeline: product_analytics\n"
        f"Failed checks: {results.get('failed', 'unknown')}\n"
        f"Details: {results}"
    )
    print(f"ALERT: {message}")
    # In production: PagerDuty / Slack critical alert
    return message


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="product_analytics_daily",
    description="Daily ETL pipeline for social media product analytics",
    default_args=default_args,
    schedule_interval="0 6 * * *",  # Daily at 06:00 UTC
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=["product-analytics", "etl", "data-engineering"],
    doc_md="""
    ## Product Analytics Daily Pipeline

    Processes daily user event data across all platforms
    (Facebook, Instagram, Messenger, WhatsApp, Threads).

    ### Pipeline Steps
    1. **Extract** raw events from data lake (Parquet)
    2. **Transform** into star schema dimensional model
    3. **Load** into DuckDB analytical warehouse
    4. **Quality Check** with automated gates
    5. **Build Aggregates** (DAU, engagement, retention)
    6. **Notify** stakeholders

    ### SLA
    - Must complete within 2 hours of scheduled start
    - Data freshness: < 24 hours

    ### Contacts
    - Owner: Data Engineering Team
    - Oncall: #data-oncall Slack channel
    """,
) as dag:

    start = EmptyOperator(task_id="start")

    extract = PythonOperator(
        task_id="extract",
        python_callable=_extract_data,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=_transform_data,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=_load_data,
    )

    quality_check = PythonOperator(
        task_id="quality_check",
        python_callable=_run_quality_checks,
    )

    quality_gate = BranchPythonOperator(
        task_id="quality_gate",
        python_callable=_check_quality_gate,
    )

    build_aggregates = PythonOperator(
        task_id="build_aggregates",
        python_callable=_build_aggregates,
    )

    quality_alert = PythonOperator(
        task_id="quality_alert",
        python_callable=_quality_alert,
    )

    notify = PythonOperator(
        task_id="notify",
        python_callable=_send_notification,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # Task dependencies
    start >> extract >> transform >> load >> quality_check >> quality_gate
    quality_gate >> [build_aggregates, quality_alert]
    build_aggregates >> notify >> end
    quality_alert >> end
