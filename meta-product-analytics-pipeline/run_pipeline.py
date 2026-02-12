#!/usr/bin/env python3
"""
Product Analytics Pipeline — Quick Start Runner
=================================================
Generates demo data, runs the full ETL pipeline, and produces
interactive analytics dashboards in a single command.

Usage:
    python run_pipeline.py                      # Full demo (5k users, 30 days)
    python run_pipeline.py --users 1000 --days 7  # Smaller demo
    python run_pipeline.py --skip-viz           # Skip visualization
"""

import argparse
import json
import logging
import os
import sys
import time

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def main():
    parser = argparse.ArgumentParser(
        description="Run the Product Analytics Data Pipeline end-to-end"
    )
    parser.add_argument("--users", type=int, default=5000, help="Number of users to generate")
    parser.add_argument("--days", type=int, default=30, help="Number of days of event data")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument("--skip-viz", action="store_true", help="Skip visualization generation")
    parser.add_argument("--output", type=str, default="data", help="Base output directory")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger = logging.getLogger("pipeline")

    total_start = time.time()

    # ── Step 1: Generate synthetic data ──────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 1: Generating synthetic social media event data")
    logger.info("  Users: %d | Days: %d | Seed: %d", args.users, args.days, args.seed)
    logger.info("=" * 60)

    from src.data_generation.generate_events import generate_demo_dataset

    raw_dir = os.path.join(args.output, "raw")
    users_df, events_dir = generate_demo_dataset(
        num_users=args.users,
        num_days=args.days,
        output_base=raw_dir,
        seed=args.seed,
    )
    logger.info("Data generation complete: %d users, events in %s", len(users_df), events_dir)

    # ── Step 2: Run ETL pipeline ─────────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 2: Running ETL Pipeline (Extract → Transform → Load)")
    logger.info("=" * 60)

    from src.etl.pipeline import ProductAnalyticsPipeline

    db_path = os.path.join(args.output, "warehouse", "product_analytics.duckdb")
    pipeline = ProductAnalyticsPipeline(raw_data_dir=raw_dir, db_path=db_path)
    report = pipeline.run_full_pipeline()

    logger.info("ETL Report:")
    logger.info(json.dumps(report, indent=2, default=str))

    # ── Step 3: Run analytics queries ────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 3: Running Product Analytics")
    logger.info("=" * 60)

    import duckdb
    from src.analytics.engagement import EngagementAnalytics
    from src.analytics.growth import GrowthAnalytics
    from src.analytics.retention import RetentionAnalytics

    conn = duckdb.connect(db_path, read_only=True)

    # Get the latest date in the dataset
    latest_date = conn.execute(
        "SELECT MAX(date_key) FROM analytics.fct_events"
    ).fetchone()[0]
    start_date = conn.execute(
        "SELECT MIN(date_key) FROM analytics.fct_events"
    ).fetchone()[0]
    report_date = str(latest_date)

    logger.info("Date range: %s to %s", start_date, latest_date)

    # Engagement
    ea = EngagementAnalytics(conn)
    dau_metrics = ea.get_dau_wau_mau(report_date)
    logger.info("DAU/WAU/MAU: %s", dau_metrics)

    cross_platform = ea.get_cross_platform_usage()
    logger.info("Cross-platform usage:\n%s", cross_platform.to_string())

    power_users = ea.get_power_user_analysis()
    logger.info("Power user analysis:\n%s", power_users.to_string())

    # Growth
    ga = GrowthAnalytics(conn)
    funnel = ga.get_funnel_analysis(report_date)
    logger.info("Engagement funnel:\n%s", funnel.to_string())

    demographics = ga.get_demographic_breakdown(report_date)
    logger.info("Demographics (top 10):\n%s", demographics.head(10).to_string())

    # Retention
    ra = RetentionAnalytics(conn)
    retention = ra.get_nday_retention([1, 3, 7])
    logger.info("N-day retention:\n%s", retention.to_string())

    retention_by_segment = ra.get_retention_by_segment(day_n=1)
    logger.info("D1 retention by segment:\n%s", retention_by_segment.to_string())

    churn_risk = ra.get_churn_risk_features()
    logger.info("Churn risk distribution:\n%s",
                churn_risk["churn_risk"].value_counts().to_string())

    conn.close()

    # ── Step 4: Generate visualizations ──────────────────────────────
    if not args.skip_viz:
        logger.info("=" * 60)
        logger.info("STEP 4: Generating Interactive Dashboards")
        logger.info("=" * 60)

        from src.visualization.dashboards import AnalyticsDashboard

        dashboard = AnalyticsDashboard(db_path=db_path)
        chart_dir = os.path.join(args.output, "processed", "charts")
        paths = dashboard.generate_all_charts(report_date, output_dir=chart_dir)
        dashboard.close()
        logger.info("Charts saved to %s", chart_dir)
        for p in paths:
            logger.info("  - %s", p)

    # ── Summary ──────────────────────────────────────────────────────
    total_elapsed = round(time.time() - total_start, 2)

    print("\n" + "=" * 60)
    print("  PRODUCT ANALYTICS PIPELINE — COMPLETE")
    print("=" * 60)
    print(f"  Users generated:    {args.users:,}")
    print(f"  Days of data:       {args.days}")
    print(f"  Total events:       {report['table_stats'].get('fct_events', 'N/A'):,}")
    print(f"  Data quality:       {report['steps']['data_quality']['pass_rate']}")
    print(f"  Total time:         {total_elapsed}s")
    print(f"  Warehouse:          {db_path}")
    if not args.skip_viz:
        print(f"  Charts:             {chart_dir}/")
    print("=" * 60)

    return report


if __name__ == "__main__":
    main()
