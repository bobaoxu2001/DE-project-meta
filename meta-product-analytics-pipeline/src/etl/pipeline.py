"""
ETL Pipeline Orchestrator
==========================
End-to-end pipeline that coordinates Extract → Transform → Load → Quality Check.
Supports both full-refresh and incremental execution modes.
"""

import logging
import os
import time
from datetime import datetime

from src.data_quality.checks import DataQualityChecker
from src.etl.extract import Extractor
from src.etl.load import Loader
from src.etl.transform import Transformer
from src.models.schema import WarehouseSchema

logger = logging.getLogger(__name__)


class ProductAnalyticsPipeline:
    """End-to-end ETL pipeline for product analytics."""

    def __init__(
        self,
        raw_data_dir: str = "data/raw",
        db_path: str = "data/warehouse/product_analytics.duckdb",
    ):
        self.raw_data_dir = raw_data_dir
        self.db_path = db_path
        self.extractor = Extractor(raw_data_dir)
        self.transformer = Transformer()

    def run_full_pipeline(self) -> dict:
        """Execute the full ETL pipeline (full refresh).

        Steps:
            1. Initialize warehouse schema
            2. Extract raw data
            3. Transform (clean, build dimensions & facts)
            4. Load into warehouse
            5. Compute aggregates
            6. Run data quality checks

        Returns:
            Pipeline execution report.
        """
        start_time = time.time()
        report = {
            "pipeline": "full_refresh",
            "started_at": datetime.utcnow().isoformat(),
            "steps": {},
        }

        logger.info("=" * 60)
        logger.info("STARTING FULL ETL PIPELINE")
        logger.info("=" * 60)

        # -- Step 1: Schema initialization --
        logger.info("[1/6] Initializing warehouse schema...")
        warehouse = WarehouseSchema(self.db_path)
        warehouse.initialize()
        warehouse.seed_dimensions()
        report["steps"]["schema_init"] = "success"

        loader = Loader(warehouse.conn)

        # -- Step 2: Extract --
        logger.info("[2/6] Extracting raw data...")
        users_df = self.extractor.extract_users()
        events_df = self.extractor.extract_events()
        report["steps"]["extract"] = {
            "users": len(users_df),
            "events": len(events_df),
        }

        # -- Step 3: Transform --
        logger.info("[3/6] Transforming data...")

        # Clean events
        clean_events = self.transformer.clean_events(events_df)

        # Build user dimension
        user_dim = self.transformer.build_user_dimension(users_df)

        # Build fact table
        fact_events = self.transformer.build_fact_events(clean_events, user_dim)

        report["steps"]["transform"] = {
            "clean_events": len(clean_events),
            "user_dim": len(user_dim),
            "fact_events": len(fact_events),
        }

        # -- Step 4: Load --
        logger.info("[4/6] Loading into warehouse...")

        loader.load_dimension(user_dim, "analytics.dim_users", mode="replace")
        loader.load_facts(fact_events, "analytics.fct_events")

        report["steps"]["load"] = "success"

        # -- Step 5: Aggregates --
        logger.info("[5/6] Computing aggregates...")

        daily_agg = self.transformer.compute_daily_aggregates(fact_events, user_dim)
        loader.load_aggregates(daily_agg, "analytics.agg_daily_metrics")

        # Compute engagement scores for the latest date
        latest_date = str(fact_events["date_key"].max())
        engagement = self.transformer.compute_engagement_scores(fact_events, latest_date)
        if not engagement.empty:
            loader.load_aggregates(engagement, "analytics.agg_user_engagement")

        report["steps"]["aggregates"] = {
            "daily_metrics": len(daily_agg),
            "engagement_scores": len(engagement),
        }

        # -- Step 6: Data Quality --
        logger.info("[6/6] Running data quality checks...")
        dq_checker = DataQualityChecker(warehouse.conn)
        dq_results = dq_checker.run_all_checks()
        report["steps"]["data_quality"] = dq_results

        # -- Summary --
        elapsed = round(time.time() - start_time, 2)
        report["elapsed_seconds"] = elapsed
        report["status"] = "success"

        table_stats = warehouse.get_table_stats()
        report["table_stats"] = table_stats

        warehouse.close()

        logger.info("=" * 60)
        logger.info("PIPELINE COMPLETE in %.2f seconds", elapsed)
        logger.info("Table stats: %s", table_stats)
        logger.info("=" * 60)

        return report

    def run_incremental(self, date_str: str) -> dict:
        """Run an incremental pipeline for a specific date.

        Only processes events for the given date partition.
        """
        start_time = time.time()
        logger.info("Running incremental pipeline for %s", date_str)

        warehouse = WarehouseSchema(self.db_path)
        # Don't re-initialize schema for incremental
        loader = Loader(warehouse.conn)

        # Extract single day
        events_df = self.extractor.extract_events_for_date(date_str)

        # Load existing user dimension
        user_dim = warehouse.conn.execute(
            "SELECT * FROM analytics.dim_users"
        ).fetchdf()

        # Transform
        clean_events = self.transformer.clean_events(events_df)
        fact_events = self.transformer.build_fact_events(clean_events, user_dim)

        # Load (replace partition)
        loader.load_facts(fact_events, partition_date=date_str)

        # Recompute daily aggregates for this date
        daily_agg = self.transformer.compute_daily_aggregates(fact_events, user_dim)
        # Append instead of full replace for aggregates
        warehouse.conn.execute(
            f"DELETE FROM analytics.agg_daily_metrics WHERE date_key = '{date_str}'"
        )
        if not daily_agg.empty:
            warehouse.conn.execute(
                "INSERT INTO analytics.agg_daily_metrics SELECT * FROM daily_agg"
            )

        elapsed = round(time.time() - start_time, 2)
        warehouse.close()

        return {
            "pipeline": "incremental",
            "date": date_str,
            "events_processed": len(fact_events),
            "elapsed_seconds": elapsed,
            "status": "success",
        }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    import argparse

    parser = argparse.ArgumentParser(description="Run the Product Analytics ETL Pipeline")
    parser.add_argument(
        "--mode", choices=["full", "incremental"], default="full",
        help="Pipeline execution mode",
    )
    parser.add_argument(
        "--date", type=str, default=None,
        help="Date for incremental mode (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--raw-dir", type=str, default="data/raw",
        help="Raw data directory",
    )
    parser.add_argument(
        "--db-path", type=str,
        default="data/warehouse/product_analytics.duckdb",
        help="Warehouse database path",
    )
    args = parser.parse_args()

    pipeline = ProductAnalyticsPipeline(
        raw_data_dir=args.raw_dir,
        db_path=args.db_path,
    )

    if args.mode == "full":
        report = pipeline.run_full_pipeline()
    else:
        if not args.date:
            raise ValueError("--date required for incremental mode")
        report = pipeline.run_incremental(args.date)

    import json
    print("\n📊 Pipeline Report:")
    print(json.dumps(report, indent=2, default=str))
