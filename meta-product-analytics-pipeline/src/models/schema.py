"""
Data Warehouse Schema Manager
==============================
Manages the star-schema dimensional model using DuckDB.
Creates and validates the warehouse structure programmatically.
"""

import logging
import os

import duckdb

logger = logging.getLogger(__name__)

SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "sql")


class WarehouseSchema:
    """Manages the data warehouse schema lifecycle."""

    def __init__(self, db_path: str = "data/warehouse/product_analytics.duckdb"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = duckdb.connect(db_path)
        logger.info("Connected to DuckDB warehouse at %s", db_path)

    def initialize(self) -> None:
        """Create all schema objects (tables, indexes)."""
        ddl_path = os.path.join(SQL_DIR, "create_tables.sql")
        logger.info("Executing DDL from %s", ddl_path)

        with open(ddl_path, "r") as f:
            ddl = f.read()

        # Execute each statement
        for stmt in ddl.split(";"):
            stmt = stmt.strip()
            if stmt and not stmt.startswith("--"):
                try:
                    self.conn.execute(stmt)
                except Exception as e:
                    logger.warning("DDL statement skipped: %s — %s", stmt[:80], e)

        logger.info("Schema initialized successfully.")

    def seed_dimensions(self) -> None:
        """Populate static dimension tables (platform, event_type, date)."""
        etl_path = os.path.join(SQL_DIR, "etl_queries.sql")
        logger.info("Seeding dimension tables from %s", etl_path)

        with open(etl_path, "r") as f:
            content = f.read()

        # Execute sections 1-3 (dim_date, dim_platform, dim_event_type)
        sections = content.split("-- -----")
        for section in sections:
            # Find INSERT/CREATE statements
            lines = section.strip().split("\n")
            sql_lines = []
            in_comment_block = False
            for line in lines:
                stripped = line.strip()
                if stripped.startswith("/*"):
                    in_comment_block = True
                    continue
                if stripped.endswith("*/"):
                    in_comment_block = False
                    continue
                if in_comment_block:
                    continue
                if stripped.startswith("--"):
                    continue
                sql_lines.append(line)

            sql = "\n".join(sql_lines).strip()
            if sql and ("INSERT" in sql.upper() or "CREATE" in sql.upper()):
                # Only run the first 3 INSERT sections (dimensions)
                for stmt in sql.split(";"):
                    stmt = stmt.strip()
                    if stmt and "INSERT" in stmt.upper():
                        try:
                            self.conn.execute(stmt)
                        except Exception as e:
                            logger.warning("Seed statement warning: %s", e)

        logger.info("Dimension tables seeded.")

    def get_table_stats(self) -> dict:
        """Return row counts for all tables in the analytics schema."""
        tables = self.conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'analytics'"
        ).fetchall()

        stats = {}
        for (table_name,) in tables:
            count = self.conn.execute(
                f"SELECT COUNT(*) FROM analytics.{table_name}"
            ).fetchone()[0]
            stats[table_name] = count

        return stats

    def close(self) -> None:
        self.conn.close()
        logger.info("Warehouse connection closed.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
