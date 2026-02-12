"""
Engagement Analytics
=====================
Computes engagement metrics critical to social media product analytics:
  - DAU / WAU / MAU
  - DAU/MAU ratio (stickiness)
  - L7 / L28 engagement
  - Engagement distribution by user segment
  - Cross-platform engagement
"""

import logging
from datetime import timedelta

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


class EngagementAnalytics:
    """Compute engagement metrics from the analytics warehouse."""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def get_dau_wau_mau(self, report_date: str) -> dict:
        """Compute DAU, WAU, MAU for a given date.

        Returns dict with user counts and ratios.
        """
        result = self.conn.execute(f"""
            WITH active_events AS (
                SELECT f.user_key, f.date_key
                FROM analytics.fct_events f
                JOIN analytics.dim_event_type et
                  ON f.event_type_key = et.event_type_key
                WHERE et.is_active_event = TRUE
            )
            SELECT
                COUNT(DISTINCT CASE
                    WHEN date_key = DATE '{report_date}'
                    THEN user_key END) AS dau,
                COUNT(DISTINCT CASE
                    WHEN date_key BETWEEN DATE '{report_date}' - INTERVAL '6 days'
                                      AND DATE '{report_date}'
                    THEN user_key END) AS wau,
                COUNT(DISTINCT CASE
                    WHEN date_key BETWEEN DATE '{report_date}' - INTERVAL '27 days'
                                      AND DATE '{report_date}'
                    THEN user_key END) AS mau
            FROM active_events
        """).fetchone()

        dau, wau, mau = result
        return {
            "report_date": report_date,
            "dau": dau,
            "wau": wau,
            "mau": mau,
            "dau_mau_ratio": round(dau / mau * 100, 2) if mau > 0 else 0,
            "dau_wau_ratio": round(dau / wau * 100, 2) if wau > 0 else 0,
        }

    def get_dau_trend(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Get daily DAU trend with moving averages."""
        df = self.conn.execute(f"""
            SELECT
                date_key,
                SUM(dau) AS dau,
                SUM(total_events) AS total_events,
                SUM(total_sessions) AS total_sessions
            FROM analytics.agg_daily_metrics
            WHERE date_key BETWEEN DATE '{start_date}' AND DATE '{end_date}'
            GROUP BY date_key
            ORDER BY date_key
        """).fetchdf()

        if not df.empty:
            df["dau_7d_ma"] = df["dau"].rolling(7, min_periods=1).mean().round(0)
            df["events_per_dau"] = (df["total_events"] / df["dau"]).round(1)

        return df

    def get_engagement_distribution(self) -> pd.DataFrame:
        """Get distribution of engagement scores across user segments."""
        return self.conn.execute("""
            SELECT
                u.user_segment,
                COUNT(*) AS num_users,
                ROUND(AVG(e.engagement_score), 1) AS avg_score,
                ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY e.engagement_score), 1) AS p25,
                ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY e.engagement_score), 1) AS p50,
                ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY e.engagement_score), 1) AS p75,
                ROUND(AVG(e.l7_days_active), 1) AS avg_l7_days,
                ROUND(AVG(e.platforms_used_l7), 1) AS avg_platforms
            FROM analytics.agg_user_engagement e
            JOIN analytics.dim_users u ON e.user_key = u.user_key AND u.is_current = TRUE
            GROUP BY u.user_segment
            ORDER BY avg_score DESC
        """).fetchdf()

    def get_platform_engagement(self, report_date: str) -> pd.DataFrame:
        """Get engagement metrics broken down by platform."""
        return self.conn.execute(f"""
            SELECT
                m.platform_key,
                p.platform_name,
                m.dau,
                m.total_events,
                m.total_sessions,
                m.content_creates,
                m.likes + m.comments + m.shares AS interactions,
                m.ad_impressions,
                m.ad_clicks,
                CASE WHEN m.ad_impressions > 0
                     THEN ROUND(m.ad_clicks * 100.0 / m.ad_impressions, 2)
                     ELSE 0 END AS ad_ctr_pct,
                m.avg_session_events
            FROM analytics.agg_daily_metrics m
            JOIN analytics.dim_platform p ON m.platform_key = p.platform_key
            WHERE m.date_key = DATE '{report_date}'
            ORDER BY m.dau DESC
        """).fetchdf()

    def get_cross_platform_usage(self) -> pd.DataFrame:
        """Analyze how many platforms each user uses."""
        return self.conn.execute("""
            WITH user_platforms AS (
                SELECT
                    user_key,
                    COUNT(DISTINCT platform_key) AS platforms_used
                FROM analytics.fct_events
                GROUP BY user_key
            )
            SELECT
                platforms_used,
                COUNT(*) AS num_users,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_users
            FROM user_platforms
            GROUP BY platforms_used
            ORDER BY platforms_used
        """).fetchdf()

    def get_power_user_analysis(self) -> pd.DataFrame:
        """Pareto analysis: what % of events come from top N% of users."""
        return self.conn.execute("""
            WITH user_events AS (
                SELECT
                    user_key,
                    COUNT(*) AS total_events,
                    PERCENT_RANK() OVER (ORDER BY COUNT(*) DESC) AS pct_rank
                FROM analytics.fct_events
                GROUP BY user_key
            )
            SELECT
                CASE
                    WHEN pct_rank <= 0.01 THEN 'Top 1%'
                    WHEN pct_rank <= 0.05 THEN 'Top 5%'
                    WHEN pct_rank <= 0.10 THEN 'Top 10%'
                    WHEN pct_rank <= 0.20 THEN 'Top 20%'
                    WHEN pct_rank <= 0.50 THEN 'Top 50%'
                    ELSE 'Bottom 50%'
                END AS user_tier,
                COUNT(*) AS num_users,
                SUM(total_events) AS total_events,
                ROUND(SUM(total_events) * 100.0 /
                    (SELECT SUM(total_events) FROM user_events), 2) AS pct_of_total_events
            FROM user_events
            GROUP BY
                CASE
                    WHEN pct_rank <= 0.01 THEN 'Top 1%'
                    WHEN pct_rank <= 0.05 THEN 'Top 5%'
                    WHEN pct_rank <= 0.10 THEN 'Top 10%'
                    WHEN pct_rank <= 0.20 THEN 'Top 20%'
                    WHEN pct_rank <= 0.50 THEN 'Top 50%'
                    ELSE 'Bottom 50%'
                END
            ORDER BY MIN(pct_rank)
        """).fetchdf()
