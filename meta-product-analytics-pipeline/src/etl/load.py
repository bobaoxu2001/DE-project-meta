"""
Load Layer
===========
Loads transformed data into the DuckDB analytical warehouse.
Supports full-refresh and incremental loading strategies.
"""

import logging
from typing import Literal

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


class Loader:
    """Loads data into the DuckDB warehouse."""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def load_dimension(
        self,
        df: pd.DataFrame,
        table_name: str,
        mode: Literal["replace", "append", "upsert"] = "replace",
        key_column: str | None = None,
    ) -> int:
        """Load a dimension table.

        Args:
            df: DataFrame to load.
            table_name: Target table (e.g., 'analytics.dim_users').
            mode: 'replace' drops and recreates, 'append' inserts,
                  'upsert' updates existing rows by key.
            key_column: Primary key column for upsert mode.

        Returns:
            Number of rows loaded.
        """
        logger.info(
            "Loading %d rows into %s (mode=%s)...", len(df), table_name, mode
        )

        if mode == "replace":
            self.conn.execute(f"DELETE FROM {table_name}")
            self.conn.execute(
                f"INSERT INTO {table_name} SELECT * FROM df"
            )
        elif mode == "append":
            self.conn.execute(
                f"INSERT INTO {table_name} SELECT * FROM df"
            )
        elif mode == "upsert":
            if key_column is None:
                raise ValueError("key_column required for upsert mode")
            # Delete existing rows that match incoming keys
            temp_name = f"__temp_{table_name.split('.')[-1]}"
            self.conn.register(temp_name, df)
            self.conn.execute(
                f"DELETE FROM {table_name} WHERE {key_column} IN "
                f"(SELECT {key_column} FROM {temp_name})"
            )
            self.conn.execute(
                f"INSERT INTO {table_name} SELECT * FROM {temp_name}"
            )
            self.conn.unregister(temp_name)

        count = self.conn.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        ).fetchone()[0]
        logger.info("Table %s now has %d rows", table_name, count)
        return count

    def load_facts(
        self,
        df: pd.DataFrame,
        table_name: str = "analytics.fct_events",
        partition_date: str | None = None,
    ) -> int:
        """Load fact data, optionally replacing a specific date partition.

        Args:
            df: Fact DataFrame.
            table_name: Target fact table.
            partition_date: If provided, delete existing data for this date
                            before inserting (incremental load).

        Returns:
            Number of rows loaded.
        """
        if partition_date:
            logger.info(
                "Incremental load: replacing partition %s in %s",
                partition_date, table_name,
            )
            self.conn.execute(
                f"DELETE FROM {table_name} WHERE _partition_date = '{partition_date}'"
            )

        self.conn.execute(
            f"INSERT INTO {table_name} SELECT * FROM df"
        )

        count = self.conn.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        ).fetchone()[0]
        logger.info(
            "Loaded %d rows into %s (total: %d)", len(df), table_name, count
        )
        return count

    def load_aggregates(
        self,
        df: pd.DataFrame,
        table_name: str,
    ) -> int:
        """Load aggregate table (full replace)."""
        logger.info("Loading %d aggregate rows into %s...", len(df), table_name)
        self.conn.execute(f"DELETE FROM {table_name}")
        self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")

        count = self.conn.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        ).fetchone()[0]
        logger.info("Aggregate table %s: %d rows", table_name, count)
        return count

    def execute_sql_file(self, sql_path: str) -> None:
        """Execute a SQL file containing multiple statements."""
        logger.info("Executing SQL file: %s", sql_path)
        with open(sql_path, "r") as f:
            content = f.read()

        # Skip comment blocks
        in_comment = False
        statements = []
        current = []
        for line in content.split("\n"):
            stripped = line.strip()
            if stripped.startswith("/*"):
                in_comment = True
                continue
            if stripped.endswith("*/"):
                in_comment = False
                continue
            if in_comment or stripped.startswith("--"):
                continue
            current.append(line)
            if stripped.endswith(";"):
                stmt = "\n".join(current).strip().rstrip(";").strip()
                if stmt:
                    statements.append(stmt)
                current = []

        for i, stmt in enumerate(statements):
            try:
                self.conn.execute(stmt)
                logger.debug("Statement %d executed successfully", i + 1)
            except Exception as e:
                logger.warning("Statement %d failed: %s", i + 1, e)

    def verify_load(self, table_name: str) -> dict:
        """Run basic verification on a loaded table."""
        count = self.conn.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        ).fetchone()[0]

        # Sample preview
        sample = self.conn.execute(
            f"SELECT * FROM {table_name} LIMIT 5"
        ).fetchdf()

        return {
            "table": table_name,
            "row_count": count,
            "sample": sample,
        }
