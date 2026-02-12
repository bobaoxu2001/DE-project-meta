"""
Tests for Analytics Modules
=============================
"""

import os
import tempfile
import shutil

import pandas as pd
import pytest

from src.data_generation.generate_events import UserGenerator, EventGenerator
from src.etl.transform import Transformer
from src.etl.load import Loader
from src.models.schema import WarehouseSchema
from src.analytics.engagement import EngagementAnalytics
from src.analytics.growth import GrowthAnalytics
from src.analytics.retention import RetentionAnalytics


@pytest.fixture
def analytics_warehouse():
    """Create a warehouse with multi-day data for analytics testing."""
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "test_analytics.duckdb")

    ws = WarehouseSchema(db_path)
    ws.initialize()
    ws.seed_dimensions()

    # Generate data
    user_gen = UserGenerator(num_users=200, seed=42)
    users_df = user_gen.generate()
    user_dim = Transformer.build_user_dimension(users_df)

    loader = Loader(ws.conn)
    loader.load_dimension(user_dim, "analytics.dim_users")

    # Generate events for 7 days
    event_gen = EventGenerator(users_df=users_df, num_days=7, seed=42)
    all_facts = []
    for day_offset in range(7):
        date = pd.Timestamp("2025-11-01") + pd.Timedelta(days=day_offset)
        events = event_gen._events_for_day(date)
        clean = Transformer.clean_events(events)
        fact = Transformer.build_fact_events(clean, user_dim)
        all_facts.append(fact)

    combined_facts = pd.concat(all_facts, ignore_index=True)
    loader.load_facts(combined_facts)

    # Build aggregates
    daily_agg = Transformer.compute_daily_aggregates(combined_facts, user_dim)
    loader.load_aggregates(daily_agg, "analytics.agg_daily_metrics")

    # Build engagement scores
    engagement = Transformer.compute_engagement_scores(combined_facts, "2025-11-07")
    if not engagement.empty:
        loader.load_aggregates(engagement, "analytics.agg_user_engagement")

    yield ws

    ws.close()
    shutil.rmtree(temp_dir)


class TestEngagementAnalytics:
    def test_dau_wau_mau(self, analytics_warehouse):
        ea = EngagementAnalytics(analytics_warehouse.conn)
        metrics = ea.get_dau_wau_mau("2025-11-07")
        assert metrics["dau"] > 0
        assert metrics["wau"] >= metrics["dau"]
        assert metrics["dau_mau_ratio"] > 0

    def test_dau_trend(self, analytics_warehouse):
        ea = EngagementAnalytics(analytics_warehouse.conn)
        trend = ea.get_dau_trend("2025-11-01", "2025-11-07")
        assert len(trend) > 0
        assert "dau" in trend.columns
        assert "dau_7d_ma" in trend.columns

    def test_cross_platform_usage(self, analytics_warehouse):
        ea = EngagementAnalytics(analytics_warehouse.conn)
        result = ea.get_cross_platform_usage()
        assert len(result) > 0
        assert "platforms_used" in result.columns

    def test_power_user_analysis(self, analytics_warehouse):
        ea = EngagementAnalytics(analytics_warehouse.conn)
        result = ea.get_power_user_analysis()
        assert len(result) > 0
        assert "pct_of_total_events" in result.columns


class TestGrowthAnalytics:
    def test_growth_accounting(self, analytics_warehouse):
        ga = GrowthAnalytics(analytics_warehouse.conn)
        result = ga.get_growth_accounting("2025-11-01", "2025-11-07")
        assert len(result) > 0
        assert "new_users" in result.columns
        assert "retained" in result.columns

    def test_funnel_analysis(self, analytics_warehouse):
        ga = GrowthAnalytics(analytics_warehouse.conn)
        result = ga.get_funnel_analysis("2025-11-05")
        assert len(result) > 0
        assert "viewers" in result.columns

    def test_demographic_breakdown(self, analytics_warehouse):
        ga = GrowthAnalytics(analytics_warehouse.conn)
        result = ga.get_demographic_breakdown("2025-11-05")
        assert len(result) > 0
        assert "age_group" in result.columns


class TestRetentionAnalytics:
    def test_nday_retention(self, analytics_warehouse):
        ra = RetentionAnalytics(analytics_warehouse.conn)
        result = ra.get_nday_retention(retention_days=[1, 3])
        assert len(result) > 0
        assert "cohort_size" in result.columns

    def test_retention_by_segment(self, analytics_warehouse):
        ra = RetentionAnalytics(analytics_warehouse.conn)
        result = ra.get_retention_by_segment(day_n=1)
        assert len(result) > 0
        assert "retention_pct" in result.columns

    def test_retention_by_platform(self, analytics_warehouse):
        ra = RetentionAnalytics(analytics_warehouse.conn)
        result = ra.get_retention_by_platform(day_n=1)
        assert len(result) > 0
        assert "primary_platform" in result.columns
