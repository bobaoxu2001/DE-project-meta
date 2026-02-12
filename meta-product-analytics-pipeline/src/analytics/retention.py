"""
Retention Analytics
====================
Cohort-based retention analysis for product analytics.

Implements:
  - N-day retention curves (D1, D7, D14, D30)
  - Weekly cohort retention matrix
  - Retention by user segment, platform, and country
  - Churn prediction features
"""

import logging

import duckdb
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class RetentionAnalytics:
    """Retention and cohort analysis for product teams."""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def get_nday_retention(self, retention_days: list[int] | None = None) -> pd.DataFrame:
        """Compute D-N retention for weekly signup cohorts.

        Args:
            retention_days: List of day offsets to check (default: [1,3,7,14,30]).

        Returns:
            DataFrame with cohort_week, cohort_size, and retention for each N.
        """
        if retention_days is None:
            retention_days = [1, 3, 7, 14, 30]

        # Build CASE expressions for each retention day
        cases = []
        for n in retention_days:
            cases.append(f"""
                COUNT(DISTINCT CASE
                    WHEN a.date_key BETWEEN nu.cohort_date + INTERVAL '{n} days'
                                        AND nu.cohort_date + INTERVAL '{n} days'
                    THEN nu.user_key END) AS retained_d{n}
            """)

        rate_cols = []
        for n in retention_days:
            rate_cols.append(
                f"ROUND(retained_d{n} * 100.0 / NULLIF(cohort_size, 0), 2) AS d{n}_pct"
            )

        sql = f"""
            WITH new_users AS (
                SELECT
                    user_key,
                    signup_date AS cohort_date,
                    DATE_TRUNC('week', signup_date) AS cohort_week
                FROM analytics.dim_users
                WHERE is_current = TRUE
            ),
            activity AS (
                SELECT DISTINCT user_key, date_key
                FROM analytics.fct_events
            ),
            retention_raw AS (
                SELECT
                    nu.cohort_week,
                    COUNT(DISTINCT nu.user_key) AS cohort_size,
                    {', '.join(cases)}
                FROM new_users nu
                LEFT JOIN activity a ON nu.user_key = a.user_key
                GROUP BY nu.cohort_week
            )
            SELECT
                cohort_week,
                cohort_size,
                {', '.join([f'retained_d{n}' for n in retention_days])},
                {', '.join(rate_cols)}
            FROM retention_raw
            ORDER BY cohort_week
        """

        return self.conn.execute(sql).fetchdf()

    def get_weekly_retention_matrix(self) -> pd.DataFrame:
        """Build a full weekly cohort retention matrix.

        Returns:
            Pivot table: rows = cohort_week, columns = weeks_since_signup.
        """
        df = self.conn.execute("""
            SELECT
                cohort_week,
                weeks_since_signup,
                cohort_size,
                retained_users,
                retention_rate
            FROM analytics.agg_retention_cohorts
            WHERE platform_key = (
                SELECT platform_key FROM analytics.agg_retention_cohorts
                GROUP BY platform_key ORDER BY SUM(cohort_size) DESC LIMIT 1
            )
            ORDER BY cohort_week, weeks_since_signup
        """).fetchdf()

        if df.empty:
            logger.warning("No retention cohort data found. Run ETL first.")
            return df

        # Pivot into matrix form
        matrix = df.pivot_table(
            index="cohort_week",
            columns="weeks_since_signup",
            values="retention_rate",
            aggfunc="first",
        )
        matrix.columns = [f"Week {int(c)}" for c in matrix.columns]

        return matrix

    def get_retention_by_segment(self, day_n: int = 7) -> pd.DataFrame:
        """Get D-N retention broken down by user segment."""
        return self.conn.execute(f"""
            WITH new_users AS (
                SELECT
                    user_key,
                    signup_date AS cohort_date,
                    user_segment
                FROM analytics.dim_users
                WHERE is_current = TRUE
            ),
            activity AS (
                SELECT DISTINCT user_key, date_key
                FROM analytics.fct_events
            )
            SELECT
                nu.user_segment,
                COUNT(DISTINCT nu.user_key) AS cohort_size,
                COUNT(DISTINCT CASE
                    WHEN a.date_key = nu.cohort_date + INTERVAL '{day_n} days'
                    THEN nu.user_key END) AS retained,
                ROUND(COUNT(DISTINCT CASE
                    WHEN a.date_key = nu.cohort_date + INTERVAL '{day_n} days'
                    THEN nu.user_key END) * 100.0 /
                    NULLIF(COUNT(DISTINCT nu.user_key), 0), 2) AS retention_pct
            FROM new_users nu
            LEFT JOIN activity a ON nu.user_key = a.user_key
            GROUP BY nu.user_segment
            ORDER BY retention_pct DESC
        """).fetchdf()

    def get_retention_by_platform(self, day_n: int = 7) -> pd.DataFrame:
        """Get D-N retention broken down by primary platform."""
        return self.conn.execute(f"""
            WITH new_users AS (
                SELECT
                    user_key,
                    signup_date AS cohort_date,
                    primary_platform
                FROM analytics.dim_users
                WHERE is_current = TRUE
            ),
            activity AS (
                SELECT DISTINCT user_key, date_key
                FROM analytics.fct_events
            )
            SELECT
                nu.primary_platform,
                COUNT(DISTINCT nu.user_key) AS cohort_size,
                COUNT(DISTINCT CASE
                    WHEN a.date_key = nu.cohort_date + INTERVAL '{day_n} days'
                    THEN nu.user_key END) AS retained,
                ROUND(COUNT(DISTINCT CASE
                    WHEN a.date_key = nu.cohort_date + INTERVAL '{day_n} days'
                    THEN nu.user_key END) * 100.0 /
                    NULLIF(COUNT(DISTINCT nu.user_key), 0), 2) AS retention_pct
            FROM new_users nu
            LEFT JOIN activity a ON nu.user_key = a.user_key
            GROUP BY nu.primary_platform
            ORDER BY retention_pct DESC
        """).fetchdf()

    def get_churn_risk_features(self) -> pd.DataFrame:
        """Generate features for churn prediction.

        Returns per-user features useful for ML churn models:
          - Days since last active
          - Activity trend (declining, stable, increasing)
          - L7 vs L28 activity ratio
        """
        return self.conn.execute("""
            WITH user_activity AS (
                SELECT
                    e.user_key,
                    e.date_key,
                    e.engagement_score,
                    e.l7_days_active,
                    e.l28_days_active,
                    e.total_events_l7,
                    e.total_events_l28,
                    e.platforms_used_l7
                FROM analytics.agg_user_engagement e
            )
            SELECT
                user_key,
                engagement_score,
                l7_days_active,
                l28_days_active,
                ROUND(l7_days_active * 4.0 / NULLIF(l28_days_active, 0), 2) AS l7_l28_ratio,
                total_events_l7,
                total_events_l28,
                ROUND(total_events_l7 * 4.0 / NULLIF(total_events_l28, 0), 2) AS event_trend_ratio,
                platforms_used_l7,
                CASE
                    WHEN l7_days_active = 0 THEN 'churned'
                    WHEN l7_days_active <= 2 AND engagement_score < 30 THEN 'high_risk'
                    WHEN l7_days_active <= 4 AND engagement_score < 50 THEN 'medium_risk'
                    ELSE 'low_risk'
                END AS churn_risk
            FROM user_activity
            ORDER BY engagement_score ASC
        """).fetchdf()
