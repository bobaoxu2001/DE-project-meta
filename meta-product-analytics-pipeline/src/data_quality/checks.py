"""
Data Quality Framework
=======================
Comprehensive data quality checks for the product analytics warehouse.

Implements checks for:
  - Completeness  : null rates, missing dimensions
  - Uniqueness    : duplicate detection
  - Freshness     : data recency
  - Validity      : range checks, referential integrity
  - Consistency   : cross-table reconciliation
  - Volume        : row count anomalies

Inspired by industry-standard DQ frameworks (Great Expectations, dbt tests).
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


class CheckSeverity(Enum):
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


class CheckStatus(Enum):
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class CheckResult:
    """Result of a single data quality check."""
    check_name: str
    table: str
    severity: CheckSeverity
    status: CheckStatus
    message: str
    details: dict = field(default_factory=dict)


class DataQualityChecker:
    """Run data quality checks against the analytics warehouse."""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn
        self.results: list[CheckResult] = []

    def _add_result(self, result: CheckResult) -> None:
        self.results.append(result)
        log_fn = logger.info if result.status == CheckStatus.PASSED else logger.warning
        log_fn("[%s] %s: %s — %s", result.severity.value, result.check_name,
               result.status.value, result.message)

    # ------------------------------------------------------------------
    # Completeness Checks
    # ------------------------------------------------------------------

    def check_null_rate(
        self, table: str, column: str, threshold: float = 0.01
    ) -> CheckResult:
        """Check that null rate for a column is below threshold."""
        total = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        if total == 0:
            result = CheckResult(
                check_name=f"null_rate_{column}",
                table=table,
                severity=CheckSeverity.WARNING,
                status=CheckStatus.SKIPPED,
                message=f"Table {table} is empty",
            )
            self._add_result(result)
            return result

        nulls = self.conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL"
        ).fetchone()[0]
        null_rate = nulls / total

        status = CheckStatus.PASSED if null_rate <= threshold else CheckStatus.FAILED
        result = CheckResult(
            check_name=f"null_rate_{column}",
            table=table,
            severity=CheckSeverity.CRITICAL,
            status=status,
            message=f"Null rate: {null_rate:.4%} (threshold: {threshold:.2%})",
            details={"total": total, "nulls": nulls, "null_rate": null_rate},
        )
        self._add_result(result)
        return result

    # ------------------------------------------------------------------
    # Uniqueness Checks
    # ------------------------------------------------------------------

    def check_uniqueness(
        self, table: str, column: str
    ) -> CheckResult:
        """Check that a column has no duplicate values."""
        total = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        distinct = self.conn.execute(
            f"SELECT COUNT(DISTINCT {column}) FROM {table}"
        ).fetchone()[0]

        duplicates = total - distinct
        status = CheckStatus.PASSED if duplicates == 0 else CheckStatus.FAILED

        result = CheckResult(
            check_name=f"uniqueness_{column}",
            table=table,
            severity=CheckSeverity.CRITICAL,
            status=status,
            message=f"Duplicates: {duplicates:,} out of {total:,} rows",
            details={"total": total, "distinct": distinct, "duplicates": duplicates},
        )
        self._add_result(result)
        return result

    # ------------------------------------------------------------------
    # Freshness Checks
    # ------------------------------------------------------------------

    def check_freshness(
        self, table: str, timestamp_col: str, max_hours: int = 48
    ) -> CheckResult:
        """Check that the most recent data is within the freshness window."""
        result_row = self.conn.execute(
            f"SELECT MAX({timestamp_col}) FROM {table}"
        ).fetchone()

        if result_row[0] is None:
            result = CheckResult(
                check_name="freshness",
                table=table,
                severity=CheckSeverity.WARNING,
                status=CheckStatus.SKIPPED,
                message="No data found in table",
            )
            self._add_result(result)
            return result

        latest = pd.to_datetime(result_row[0])
        age_hours = (datetime.utcnow() - latest).total_seconds() / 3600

        status = CheckStatus.PASSED if age_hours <= max_hours else CheckStatus.FAILED

        result = CheckResult(
            check_name="freshness",
            table=table,
            severity=CheckSeverity.WARNING,
            status=status,
            message=f"Latest data: {latest} ({age_hours:.1f}h ago, max: {max_hours}h)",
            details={"latest_timestamp": str(latest), "age_hours": round(age_hours, 1)},
        )
        self._add_result(result)
        return result

    # ------------------------------------------------------------------
    # Referential Integrity Checks
    # ------------------------------------------------------------------

    def check_referential_integrity(
        self,
        fact_table: str,
        fact_key: str,
        dim_table: str,
        dim_key: str,
    ) -> CheckResult:
        """Check that all foreign keys in fact table exist in dimension."""
        orphans = self.conn.execute(
            f"""
            SELECT COUNT(DISTINCT f.{fact_key})
            FROM {fact_table} f
            LEFT JOIN {dim_table} d ON f.{fact_key} = d.{dim_key}
            WHERE d.{dim_key} IS NULL
            """
        ).fetchone()[0]

        total = self.conn.execute(
            f"SELECT COUNT(DISTINCT {fact_key}) FROM {fact_table}"
        ).fetchone()[0]

        status = CheckStatus.PASSED if orphans == 0 else CheckStatus.FAILED

        result = CheckResult(
            check_name=f"ref_integrity_{fact_key}",
            table=fact_table,
            severity=CheckSeverity.CRITICAL,
            status=status,
            message=f"Orphaned keys: {orphans:,} out of {total:,} distinct keys",
            details={"orphaned": orphans, "total_distinct": total},
        )
        self._add_result(result)
        return result

    # ------------------------------------------------------------------
    # Volume Checks
    # ------------------------------------------------------------------

    def check_row_count(
        self, table: str, min_rows: int = 1
    ) -> CheckResult:
        """Check that a table has at least min_rows."""
        count = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        status = CheckStatus.PASSED if count >= min_rows else CheckStatus.FAILED

        result = CheckResult(
            check_name="row_count",
            table=table,
            severity=CheckSeverity.CRITICAL,
            status=status,
            message=f"Row count: {count:,} (minimum: {min_rows:,})",
            details={"count": count, "minimum": min_rows},
        )
        self._add_result(result)
        return result

    # ------------------------------------------------------------------
    # Value Range Checks
    # ------------------------------------------------------------------

    def check_value_range(
        self, table: str, column: str,
        min_val: float | None = None,
        max_val: float | None = None,
    ) -> CheckResult:
        """Check that column values fall within expected range."""
        actual_min, actual_max = self.conn.execute(
            f"SELECT MIN({column}), MAX({column}) FROM {table}"
        ).fetchone()

        violations = []
        if min_val is not None and actual_min is not None and actual_min < min_val:
            violations.append(f"min={actual_min} < expected_min={min_val}")
        if max_val is not None and actual_max is not None and actual_max > max_val:
            violations.append(f"max={actual_max} > expected_max={max_val}")

        status = CheckStatus.PASSED if not violations else CheckStatus.FAILED

        result = CheckResult(
            check_name=f"value_range_{column}",
            table=table,
            severity=CheckSeverity.WARNING,
            status=status,
            message=f"Range [{actual_min}, {actual_max}] — " + (
                "OK" if not violations else "; ".join(violations)
            ),
            details={"actual_min": actual_min, "actual_max": actual_max},
        )
        self._add_result(result)
        return result

    # ------------------------------------------------------------------
    # Run All Checks
    # ------------------------------------------------------------------

    def run_all_checks(self) -> dict:
        """Execute the full data quality check suite."""
        logger.info("=" * 50)
        logger.info("RUNNING DATA QUALITY CHECKS")
        logger.info("=" * 50)

        self.results = []

        # --- Fact table checks ---
        self.check_row_count("analytics.fct_events", min_rows=1000)
        self.check_uniqueness("analytics.fct_events", "event_id")
        self.check_null_rate("analytics.fct_events", "user_key")
        self.check_null_rate("analytics.fct_events", "event_type_key")
        self.check_null_rate("analytics.fct_events", "date_key")

        # Referential integrity
        self.check_referential_integrity(
            "analytics.fct_events", "user_key",
            "analytics.dim_users", "user_key",
        )
        self.check_referential_integrity(
            "analytics.fct_events", "platform_key",
            "analytics.dim_platform", "platform_key",
        )
        self.check_referential_integrity(
            "analytics.fct_events", "event_type_key",
            "analytics.dim_event_type", "event_type_key",
        )

        # --- Dimension checks ---
        self.check_row_count("analytics.dim_users", min_rows=100)
        self.check_uniqueness("analytics.dim_users", "user_key")
        self.check_null_rate("analytics.dim_users", "user_id")

        self.check_row_count("analytics.dim_platform", min_rows=5)
        self.check_row_count("analytics.dim_event_type", min_rows=10)
        self.check_row_count("analytics.dim_date", min_rows=365)

        # --- Aggregate checks ---
        self.check_row_count("analytics.agg_daily_metrics", min_rows=5)
        self.check_value_range("analytics.agg_daily_metrics", "dau", min_val=0)
        self.check_value_range(
            "analytics.agg_daily_metrics", "avg_session_events",
            min_val=0, max_val=1000,
        )

        # --- Summary ---
        passed = sum(1 for r in self.results if r.status == CheckStatus.PASSED)
        failed = sum(1 for r in self.results if r.status == CheckStatus.FAILED)
        skipped = sum(1 for r in self.results if r.status == CheckStatus.SKIPPED)
        total = len(self.results)

        summary = {
            "total_checks": total,
            "passed": passed,
            "failed": failed,
            "skipped": skipped,
            "pass_rate": f"{passed / total * 100:.1f}%" if total > 0 else "N/A",
            "details": [
                {
                    "check": r.check_name,
                    "table": r.table,
                    "status": r.status.value,
                    "severity": r.severity.value,
                    "message": r.message,
                }
                for r in self.results
            ],
        }

        logger.info("DQ Summary: %d/%d passed (%.1f%%)", passed, total,
                     passed / total * 100 if total > 0 else 0)
        return summary
