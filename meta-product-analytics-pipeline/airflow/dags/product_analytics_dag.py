"""
Airflow DAG — Product Analytics ETL Pipeline
=============================================
Orchestrates the daily ETL pipeline for product analytics:

    extract → transform → load → quality_check → build_aggregates → notify

Schedule: Daily at 06:00 UTC
Retry: 3 attempts with 5-minute delay
SLA: Must complete within 2 hours

This DAG demonstrates production-grade pipeline orchestration with:
  - Idempotent tasks (safe to re-run)
  - Data quality gates (pipeline halts on critical failures)
  - Incremental processing (date-partitioned)
  - Configuration-driven via pipeline_config.yaml
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


def _get_config():
    """Load pipeline config. Imported inside tasks so Airflow doesn't need src at parse time."""
    from src import config as cfg
    return {
        "raw_data_dir": "data/raw",
        "db_path": cfg.get("database", "path", "data/warehouse/product_analytics.duckdb"),
    }


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def _extract_data(**context):
    """Extract raw data from the data lake for the execution date."""
    from src.etl.extract import Extractor

    ds = context["ds"]
    conf = _get_config()
    extractor = Extractor(raw_data_dir=conf["raw_data_dir"])

    events_df = extractor.extract_events_for_date(ds)
    users_df = extractor.extract_users()

    context["ti"].xcom_push(key="events_count", value=len(events_df))
    context["ti"].xcom_push(key="users_count", value=len(users_df))
    return {"events": len(events_df), "users": len(users_df)}


def _transform_and_load(**context):
    """Extract, transform, and load data for the execution date (incremental)."""
    from src.etl.extract import Extractor
    from src.etl.load import Loader
    from src.etl.transform import Transformer
    from src.models.schema import WarehouseSchema

    ds = context["ds"]
    conf = _get_config()

    extractor = Extractor(raw_data_dir=conf["raw_data_dir"])
    transformer = Transformer()

    events_df = extractor.extract_events_for_date(ds)
    users_df = extractor.extract_users()

    clean_events = transformer.clean_events(events_df)
    user_dim = transformer.build_user_dimension(users_df)
    fact_events = transformer.build_fact_events(clean_events, user_dim)

    warehouse = WarehouseSchema(conf["db_path"])
    loader = Loader(warehouse.conn)

    loader.load_dimension(user_dim, "analytics.dim_users", mode="upsert", key_column="user_key")
    loader.load_facts(fact_events, partition_date=ds)

    context["ti"].xcom_push(key="fact_count", value=len(fact_events))
    warehouse.close()
    return {"facts_loaded": len(fact_events)}


def _run_quality_checks(**context):
    """Run data quality checks and decide if pipeline should continue."""
    from src.data_quality.checks import DataQualityChecker
    from src.models.schema import WarehouseSchema

    conf = _get_config()
    warehouse = WarehouseSchema(conf["db_path"])
    checker = DataQualityChecker(warehouse.conn)
    results = checker.run_all_checks()
    warehouse.close()

    context["ti"].xcom_push(key="dq_results", value=results)
    return results


def _check_quality_gate(**context):
    """Branch: continue if quality checks pass, else alert."""
    results = context["ti"].xcom_pull(task_ids="quality_check", key="dq_results")
    if results and results.get("failed", 0) == 0:
        return "build_aggregates"
    return "quality_alert"


def _build_aggregates(**context):
    """Compute aggregate tables for the execution date."""
    from src.etl.transform import Transformer
    from src.etl.load import Loader
    from src.models.schema import WarehouseSchema

    ds = context["ds"]
    conf = _get_config()
    warehouse = WarehouseSchema(conf["db_path"])
    loader = Loader(warehouse.conn)

    fact_df = warehouse.conn.execute(f"""
        SELECT * FROM analytics.fct_events
        WHERE _partition_date = DATE '{ds}'
    """).fetchdf()

    user_dim = warehouse.conn.execute(
        "SELECT * FROM analytics.dim_users WHERE is_current = TRUE"
    ).fetchdf()

    if not fact_df.empty:
        transformer = Transformer()

        daily_agg = transformer.compute_daily_aggregates(fact_df, user_dim)
        import duckdb
        try:
            warehouse.conn.execute(
                f"DELETE FROM analytics.agg_daily_metrics WHERE date_key = DATE '{ds}'"
            )
        except duckdb.CatalogException:
            pass
        loader.load_aggregates(daily_agg, "analytics.agg_daily_metrics")

        engagement = transformer.compute_engagement_scores(fact_df, ds)
        if not engagement.empty:
            loader.load_aggregates(engagement, "analytics.agg_user_engagement")

    warehouse.close()
    return {"aggregates_built": True}


def _send_notification(**context):
    """Send pipeline completion notification."""
    ds = context["ds"]
    ti = context["ti"]

    events_count = ti.xcom_pull(task_ids="extract", key="events_count") or 0
    fact_count = ti.xcom_pull(task_ids="transform_and_load", key="fact_count") or 0

    message = (
        f"Product Analytics Pipeline Complete\n"
        f"Date: {ds}\n"
        f"Events extracted: {events_count:,}\n"
        f"Facts loaded: {fact_count:,}\n"
        f"Status: SUCCESS"
    )
    print(message)
    return message


def _quality_alert(**context):
    """Alert on data quality failures."""
    results = context["ti"].xcom_pull(task_ids="quality_check", key="dq_results")

    message = (
        f"DATA QUALITY ALERT\n"
        f"Pipeline: product_analytics\n"
        f"Failed checks: {results.get('failed', 'unknown')}\n"
        f"Pass rate: {results.get('pass_rate', 'N/A')}\n"
        f"Details: {results}"
    )
    print(f"ALERT: {message}")
    return message


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="product_analytics_daily",
    description="Daily ETL pipeline for social media product analytics (5 platforms, 15 event types)",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=["product-analytics", "etl", "data-engineering", "meta"],
    doc_md="""
    ## Product Analytics Daily Pipeline

    Processes daily user event data across all platforms
    (Facebook, Instagram, Messenger, WhatsApp, Threads).

    **Scale**: 10K–100K users, 50K–500K events/day, ~5M+ events/month

    ### Pipeline Steps
    1. **Extract** raw events from data lake (Parquet, date-partitioned)
    2. **Transform** into star schema dimensional model (SCD-2 users)
    3. **Load** into DuckDB analytical warehouse (incremental, idempotent)
    4. **Quality Check** with 17+ automated gates (config-driven thresholds)
    5. **Build Aggregates** (DAU, engagement scores, retention cohorts)
    6. **Notify** stakeholders

    ### Configuration
    - Thresholds and paths driven by `config/pipeline_config.yaml`
    - DQ thresholds: null < 1%, freshness < 24h, min 1000 rows/partition

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

    transform_and_load = PythonOperator(
        task_id="transform_and_load",
        python_callable=_transform_and_load,
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
    start >> extract >> transform_and_load >> quality_check >> quality_gate
    quality_gate >> [build_aggregates, quality_alert]
    build_aggregates >> notify >> end
    quality_alert >> end
