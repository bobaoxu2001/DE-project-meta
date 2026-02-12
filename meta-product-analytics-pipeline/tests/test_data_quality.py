"""
Tests for Data Quality Framework
==================================
"""

import os
import tempfile
import shutil

import duckdb
import pandas as pd
import pytest

from src.data_quality.checks import (
    DataQualityChecker,
    CheckResult,
    CheckStatus,
    CheckSeverity,
)
from src.models.schema import WarehouseSchema
from src.data_generation.generate_events import UserGenerator, EventGenerator
from src.etl.transform import Transformer
from src.etl.load import Loader


@pytest.fixture
def populated_warehouse():
    """Create a warehouse with sample data for DQ testing."""
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "test_dq.duckdb")

    ws = WarehouseSchema(db_path)
    ws.initialize()
    ws.seed_dimensions()

    # Generate and load sample data
    user_gen = UserGenerator(num_users=100, seed=42)
    users_df = user_gen.generate()
    user_dim = Transformer.build_user_dimension(users_df)

    event_gen = EventGenerator(users_df=users_df, num_days=3, seed=42)
    events_df = event_gen._events_for_day(pd.Timestamp("2025-11-01"))
    clean_events = Transformer.clean_events(events_df)
    fact_events = Transformer.build_fact_events(clean_events, user_dim)

    loader = Loader(ws.conn)
    loader.load_dimension(user_dim, "analytics.dim_users")
    loader.load_facts(fact_events)

    # Load daily aggregates
    daily_agg = Transformer.compute_daily_aggregates(fact_events, user_dim)
    loader.load_aggregates(daily_agg, "analytics.agg_daily_metrics")

    yield ws

    ws.close()
    shutil.rmtree(temp_dir)


class TestDataQualityChecks:
    def test_null_rate_passes(self, populated_warehouse):
        checker = DataQualityChecker(populated_warehouse.conn)
        result = checker.check_null_rate("analytics.fct_events", "user_key")
        assert result.status == CheckStatus.PASSED

    def test_uniqueness_passes(self, populated_warehouse):
        checker = DataQualityChecker(populated_warehouse.conn)
        result = checker.check_uniqueness("analytics.fct_events", "event_id")
        assert result.status == CheckStatus.PASSED

    def test_row_count_passes(self, populated_warehouse):
        checker = DataQualityChecker(populated_warehouse.conn)
        result = checker.check_row_count("analytics.fct_events", min_rows=10)
        assert result.status == CheckStatus.PASSED

    def test_row_count_fails(self, populated_warehouse):
        checker = DataQualityChecker(populated_warehouse.conn)
        result = checker.check_row_count(
            "analytics.fct_events", min_rows=999999999
        )
        assert result.status == CheckStatus.FAILED

    def test_referential_integrity(self, populated_warehouse):
        checker = DataQualityChecker(populated_warehouse.conn)
        result = checker.check_referential_integrity(
            "analytics.fct_events", "user_key",
            "analytics.dim_users", "user_key",
        )
        assert result.status == CheckStatus.PASSED

    def test_value_range(self, populated_warehouse):
        checker = DataQualityChecker(populated_warehouse.conn)
        result = checker.check_value_range(
            "analytics.agg_daily_metrics", "dau", min_val=0,
        )
        assert result.status == CheckStatus.PASSED

    def test_run_all_checks(self, populated_warehouse):
        checker = DataQualityChecker(populated_warehouse.conn)
        results = checker.run_all_checks()

        assert "total_checks" in results
        assert "passed" in results
        assert "failed" in results
        assert results["total_checks"] > 0
        # Most checks should pass with clean generated data
        assert results["passed"] >= results["failed"]
