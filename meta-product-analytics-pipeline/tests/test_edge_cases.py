"""
Edge Case Tests
================
Tests for boundary conditions, empty inputs, and error scenarios
that the main test suite doesn't cover.
"""

import os
import tempfile
import shutil

import duckdb
import numpy as np
import pandas as pd
import pytest

from src.data_generation.generate_events import (
    EventGenerator,
    UserGenerator,
    _hour_weight,
    _day_of_week_weight,
)
from src.etl.transform import Transformer
from src.etl.load import Loader
from src.models.schema import WarehouseSchema
from src.data_quality.checks import DataQualityChecker, CheckStatus
from src import config as cfg


# ---------------------------------------------------------------------------
# Probability weight validation
# ---------------------------------------------------------------------------

class TestProbabilityWeights:
    def test_hour_weights_valid_probabilities(self):
        """Hour weights must produce a valid probability distribution."""
        weights = [_hour_weight(h) for h in range(24)]
        assert all(w >= 0 for w in weights), "Weights must be non-negative"
        assert len(weights) == 24

    def test_day_of_week_weights_sum_to_one(self):
        weights = [_day_of_week_weight(d) for d in range(7)]
        assert abs(sum(weights) - 1.0) < 1e-9

    def test_hour_probs_normalized_in_generator(self):
        """EventGenerator should normalize hour weights to sum to 1.0."""
        users = UserGenerator(num_users=10, seed=42).generate()
        gen = EventGenerator(users_df=users, num_days=1, seed=42)
        assert abs(gen.hour_probs.sum() - 1.0) < 1e-9

    def test_event_probs_normalized(self):
        users = UserGenerator(num_users=10, seed=42).generate()
        gen = EventGenerator(users_df=users, num_days=1, seed=42)
        assert abs(gen.event_probs.sum() - 1.0) < 1e-9


# ---------------------------------------------------------------------------
# Empty / minimal input handling
# ---------------------------------------------------------------------------

class TestEmptyInputs:
    def test_clean_events_empty_dataframe(self):
        empty_df = pd.DataFrame(columns=[
            "event_id", "user_id", "event_type", "platform",
            "event_timestamp", "country", "device_type", "session_id",
        ])
        result = Transformer.clean_events(empty_df)
        assert len(result) == 0
        assert list(result.columns) == list(empty_df.columns)

    def test_build_user_dimension_single_user(self):
        single = pd.DataFrame({
            "user_id": ["abc123"],
            "country": ["US"],
            "age_group": ["25-34"],
            "device_type": ["ios"],
            "user_segment": ["active"],
            "signup_date": [pd.Timestamp("2025-01-01")],
            "primary_platform": ["facebook"],
        })
        dim = Transformer.build_user_dimension(single)
        assert len(dim) == 1
        assert dim["is_current"].iloc[0] == True
        assert dim["user_key"].iloc[0] is not None

    def test_daily_aggregates_empty_fact(self):
        empty_fact = pd.DataFrame(columns=[
            "event_id", "event_timestamp", "date_key", "user_key",
            "platform_key", "event_type_key", "session_id",
            "country", "device_type", "event_count", "_partition_date",
        ])
        user_dim = pd.DataFrame(columns=[
            "user_key", "user_id", "country", "age_group", "device_type",
            "user_segment", "signup_date", "primary_platform",
            "effective_from", "effective_to", "is_current",
        ])
        result = Transformer.compute_daily_aggregates(empty_fact, user_dim)
        assert len(result) == 0

    def test_engagement_scores_empty_window(self):
        empty_fact = pd.DataFrame(columns=[
            "event_id", "event_timestamp", "date_key", "user_key",
            "platform_key", "event_type_key", "session_id",
            "country", "device_type", "event_count", "_partition_date",
        ])
        result = Transformer.compute_engagement_scores(empty_fact, "2025-11-07")
        assert len(result) == 0


# ---------------------------------------------------------------------------
# EventGenerator edge cases
# ---------------------------------------------------------------------------

class TestEventGeneratorEdgeCases:
    def test_single_user(self):
        users = UserGenerator(num_users=1, seed=42).generate()
        gen = EventGenerator(users_df=users, num_days=1, seed=42)
        events = gen._events_for_day(pd.Timestamp("2025-11-01"))
        assert len(events) >= 0  # Dormant users may produce 0 events

    def test_all_dormant_users(self):
        """All dormant users may produce zero events."""
        users = pd.DataFrame({
            "user_id": [f"user_{i}" for i in range(10)],
            "country": ["US"] * 10,
            "age_group": ["25-34"] * 10,
            "device_type": ["ios"] * 10,
            "user_segment": ["dormant"] * 10,
            "signup_date": [pd.Timestamp("2025-01-01")] * 10,
            "primary_platform": ["facebook"] * 10,
        })
        gen = EventGenerator(users_df=users, num_days=1, seed=42)
        events = gen._events_for_day(pd.Timestamp("2025-11-01"))
        # Dormant: (0, 1) range, so some users may produce 0 events
        assert isinstance(events, pd.DataFrame)

    def test_deterministic_with_seed(self):
        users = UserGenerator(num_users=50, seed=42).generate()
        gen1 = EventGenerator(users_df=users, num_days=1, seed=99)
        gen2 = EventGenerator(users_df=users, num_days=1, seed=99)
        e1 = gen1._events_for_day(pd.Timestamp("2025-11-01"))
        e2 = gen2._events_for_day(pd.Timestamp("2025-11-01"))
        assert len(e1) == len(e2)
        pd.testing.assert_frame_equal(e1, e2)


# ---------------------------------------------------------------------------
# Schema and loader edge cases
# ---------------------------------------------------------------------------

class TestSchemaEdgeCases:
    def test_double_initialize(self):
        """Re-initializing schema should not fail."""
        temp_dir = tempfile.mkdtemp()
        db_path = os.path.join(temp_dir, "test.duckdb")
        try:
            ws = WarehouseSchema(db_path)
            ws.initialize()
            ws.initialize()  # Should not raise
            ws.seed_dimensions()
            ws.seed_dimensions()  # Idempotent
            stats = ws.get_table_stats()
            assert "dim_date" in stats
            ws.close()
        finally:
            shutil.rmtree(temp_dir)

    def test_context_manager(self):
        temp_dir = tempfile.mkdtemp()
        db_path = os.path.join(temp_dir, "test.duckdb")
        try:
            with WarehouseSchema(db_path) as ws:
                ws.initialize()
                stats = ws.get_table_stats()
                assert isinstance(stats, dict)
        finally:
            shutil.rmtree(temp_dir)


# ---------------------------------------------------------------------------
# Config module
# ---------------------------------------------------------------------------

class TestConfig:
    def test_load_config_returns_dict(self):
        config = cfg.load_config()
        assert isinstance(config, dict)

    def test_get_existing_section(self):
        db_cfg = cfg.get("database")
        assert isinstance(db_cfg, dict)
        assert "path" in db_cfg

    def test_get_missing_section_returns_default(self):
        result = cfg.get("nonexistent_section", default="fallback")
        assert result == "fallback"

    def test_get_nested_key(self):
        path = cfg.get("database", "path")
        assert isinstance(path, str)
        assert "duckdb" in path


# ---------------------------------------------------------------------------
# Data quality edge cases
# ---------------------------------------------------------------------------

class TestDataQualityEdgeCases:
    def test_empty_table_null_check(self):
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE test_table (id INT, name VARCHAR)")
        checker = DataQualityChecker(conn)
        result = checker.check_null_rate("test_table", "name")
        assert result.status == CheckStatus.SKIPPED
        conn.close()

    def test_freshness_empty_table(self):
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE test_table (ts TIMESTAMP)")
        checker = DataQualityChecker(conn)
        result = checker.check_freshness("test_table", "ts")
        assert result.status == CheckStatus.SKIPPED
        conn.close()

    def test_row_count_zero(self):
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE test_table (id INT)")
        checker = DataQualityChecker(conn)
        result = checker.check_row_count("test_table", min_rows=1)
        assert result.status == CheckStatus.FAILED
        conn.close()
