"""
Tests for ETL Pipeline Components
===================================
Comprehensive tests covering extraction, transformation, loading,
and end-to-end pipeline execution.
"""

import os
import shutil
import tempfile

import duckdb
import numpy as np
import pandas as pd
import pytest

from src.data_generation.generate_events import (
    EventGenerator,
    UserGenerator,
    generate_demo_dataset,
)
from src.etl.extract import Extractor
from src.etl.transform import Transformer
from src.etl.load import Loader
from src.models.schema import WarehouseSchema


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def temp_dir():
    """Create a temporary directory for test data."""
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d)


@pytest.fixture
def sample_users():
    """Generate a small set of test users."""
    gen = UserGenerator(num_users=100, seed=42)
    return gen.generate()


@pytest.fixture
def sample_events(sample_users):
    """Generate a small set of test events."""
    gen = EventGenerator(
        users_df=sample_users,
        start_date="2025-11-01",
        num_days=3,
        seed=42,
    )
    return gen._events_for_day(pd.Timestamp("2025-11-01"))


@pytest.fixture
def warehouse(temp_dir):
    """Create a temporary DuckDB warehouse."""
    db_path = os.path.join(temp_dir, "test.duckdb")
    ws = WarehouseSchema(db_path)
    ws.initialize()
    ws.seed_dimensions()
    yield ws
    ws.close()


# ---------------------------------------------------------------------------
# Data Generation Tests
# ---------------------------------------------------------------------------

class TestDataGeneration:
    def test_user_generation_count(self):
        gen = UserGenerator(num_users=500, seed=42)
        df = gen.generate()
        assert len(df) == 500

    def test_user_generation_columns(self, sample_users):
        expected_cols = {
            "user_id", "country", "age_group", "device_type",
            "user_segment", "signup_date", "primary_platform",
        }
        assert expected_cols.issubset(set(sample_users.columns))

    def test_user_ids_unique(self, sample_users):
        assert sample_users["user_id"].nunique() == len(sample_users)

    def test_event_generation_not_empty(self, sample_events):
        assert len(sample_events) > 0

    def test_event_columns(self, sample_events):
        expected = {
            "event_id", "user_id", "event_type", "platform",
            "event_timestamp", "country", "device_type", "session_id",
        }
        assert expected.issubset(set(sample_events.columns))

    def test_event_ids_unique(self, sample_events):
        assert sample_events["event_id"].nunique() == len(sample_events)

    def test_valid_platforms(self, sample_events):
        valid = {"facebook", "instagram", "messenger", "whatsapp", "threads"}
        assert set(sample_events["platform"].unique()).issubset(valid)

    def test_demo_dataset(self, temp_dir):
        users_df, events_dir = generate_demo_dataset(
            num_users=50, num_days=2,
            output_base=os.path.join(temp_dir, "raw"),
            seed=42,
        )
        assert len(users_df) == 50
        assert os.path.isdir(events_dir)


# ---------------------------------------------------------------------------
# Extract Tests
# ---------------------------------------------------------------------------

class TestExtract:
    def test_extract_users(self, temp_dir):
        # Setup: generate data
        generate_demo_dataset(
            num_users=50, num_days=2,
            output_base=os.path.join(temp_dir, "raw"),
        )
        extractor = Extractor(os.path.join(temp_dir, "raw"))
        users = extractor.extract_users()
        assert len(users) == 50

    def test_extract_events(self, temp_dir):
        generate_demo_dataset(
            num_users=50, num_days=2,
            output_base=os.path.join(temp_dir, "raw"),
        )
        extractor = Extractor(os.path.join(temp_dir, "raw"))
        events = extractor.extract_events()
        assert len(events) > 0

    def test_extract_date_filter(self, temp_dir):
        generate_demo_dataset(
            num_users=50, num_days=3,
            output_base=os.path.join(temp_dir, "raw"),
        )
        extractor = Extractor(os.path.join(temp_dir, "raw"))
        events = extractor.extract_events(
            start_date="2025-11-01", end_date="2025-11-01"
        )
        assert len(events) > 0

    def test_list_dates(self, temp_dir):
        generate_demo_dataset(
            num_users=50, num_days=3,
            output_base=os.path.join(temp_dir, "raw"),
        )
        extractor = Extractor(os.path.join(temp_dir, "raw"))
        dates = extractor.list_available_dates()
        assert len(dates) == 3


# ---------------------------------------------------------------------------
# Transform Tests
# ---------------------------------------------------------------------------

class TestTransform:
    def test_clean_events_removes_duplicates(self, sample_events):
        # Introduce duplicates
        duped = pd.concat([sample_events, sample_events.iloc[:5]])
        cleaned = Transformer.clean_events(duped)
        assert len(cleaned) == len(sample_events)

    def test_clean_events_removes_nulls(self, sample_events):
        events = sample_events.copy()
        events.loc[0, "event_id"] = None
        cleaned = Transformer.clean_events(events)
        assert cleaned["event_id"].isna().sum() == 0

    def test_build_user_dimension(self, sample_users):
        dim = Transformer.build_user_dimension(sample_users)
        assert "user_key" in dim.columns
        assert "is_current" in dim.columns
        assert dim["is_current"].all()
        assert dim["user_key"].nunique() == len(dim)

    def test_build_fact_events(self, sample_events, sample_users):
        user_dim = Transformer.build_user_dimension(sample_users)
        clean = Transformer.clean_events(sample_events)
        fact = Transformer.build_fact_events(clean, user_dim)

        assert "user_key" in fact.columns
        assert "date_key" in fact.columns
        assert "event_type_key" in fact.columns
        assert fact["event_count"].sum() == len(fact)

    def test_daily_aggregates(self, sample_events, sample_users):
        user_dim = Transformer.build_user_dimension(sample_users)
        clean = Transformer.clean_events(sample_events)
        fact = Transformer.build_fact_events(clean, user_dim)
        agg = Transformer.compute_daily_aggregates(fact, user_dim)

        assert "dau" in agg.columns
        assert "total_events" in agg.columns
        assert len(agg) > 0
        assert (agg["dau"] > 0).all()


# ---------------------------------------------------------------------------
# Load Tests
# ---------------------------------------------------------------------------

class TestLoad:
    def test_load_dimension(self, warehouse, sample_users):
        user_dim = Transformer.build_user_dimension(sample_users)
        loader = Loader(warehouse.conn)
        count = loader.load_dimension(user_dim, "analytics.dim_users")
        assert count == len(user_dim)

    def test_load_facts(self, warehouse, sample_events, sample_users):
        user_dim = Transformer.build_user_dimension(sample_users)
        loader = Loader(warehouse.conn)
        loader.load_dimension(user_dim, "analytics.dim_users")

        clean = Transformer.clean_events(sample_events)
        fact = Transformer.build_fact_events(clean, user_dim)
        count = loader.load_facts(fact)
        assert count == len(fact)

    def test_incremental_load(self, warehouse, sample_events, sample_users):
        user_dim = Transformer.build_user_dimension(sample_users)
        loader = Loader(warehouse.conn)
        loader.load_dimension(user_dim, "analytics.dim_users")

        clean = Transformer.clean_events(sample_events)
        fact = Transformer.build_fact_events(clean, user_dim)

        # Load twice with same partition — should not double count
        loader.load_facts(fact, partition_date="2025-11-01")
        loader.load_facts(fact, partition_date="2025-11-01")

        total = warehouse.conn.execute(
            "SELECT COUNT(*) FROM analytics.fct_events"
        ).fetchone()[0]
        assert total == len(fact)

    def test_verify_load(self, warehouse, sample_users):
        user_dim = Transformer.build_user_dimension(sample_users)
        loader = Loader(warehouse.conn)
        loader.load_dimension(user_dim, "analytics.dim_users")

        result = loader.verify_load("analytics.dim_users")
        assert result["row_count"] == len(user_dim)
        assert not result["sample"].empty


# ---------------------------------------------------------------------------
# Schema Tests
# ---------------------------------------------------------------------------

class TestSchema:
    def test_schema_initialization(self, warehouse):
        stats = warehouse.get_table_stats()
        assert "dim_date" in stats
        assert "dim_platform" in stats
        assert "fct_events" in stats

    def test_dimension_seeding(self, warehouse):
        platforms = warehouse.conn.execute(
            "SELECT COUNT(*) FROM analytics.dim_platform"
        ).fetchone()[0]
        assert platforms >= 5

        event_types = warehouse.conn.execute(
            "SELECT COUNT(*) FROM analytics.dim_event_type"
        ).fetchone()[0]
        assert event_types >= 10
