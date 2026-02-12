"""
Growth Analytics
=================
Growth accounting framework for social media platforms.

Implements:
  - Growth accounting (new / retained / resurrected / churned)
  - Net growth rate and quick ratio
  - Funnel analysis (view → engage → create)
  - Geographic and demographic growth breakdowns
"""

import logging

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


class GrowthAnalytics:
    """Growth metrics and analysis for product teams."""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def get_growth_accounting(
        self, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """Compute daily growth accounting per platform.

        Categories:
          - New: first day on platform
          - Retained: active today AND yesterday
          - Resurrected: active today, NOT yesterday, but active before
          - Churned: active yesterday, NOT today (computed as lag)
        """
        return self.conn.execute(f"""
            WITH daily_active AS (
                SELECT DISTINCT user_key, date_key, platform_key
                FROM analytics.fct_events
                WHERE date_key BETWEEN DATE '{start_date}' AND DATE '{end_date}'
            ),
            classified AS (
                SELECT
                    curr.date_key,
                    curr.platform_key,
                    curr.user_key,
                    CASE
                        WHEN u.signup_date = curr.date_key THEN 'new'
                        WHEN prev.user_key IS NOT NULL THEN 'retained'
                        ELSE 'resurrected'
                    END AS user_status
                FROM daily_active curr
                LEFT JOIN daily_active prev
                    ON curr.user_key = prev.user_key
                   AND curr.platform_key = prev.platform_key
                   AND prev.date_key = curr.date_key - INTERVAL '1 day'
                JOIN analytics.dim_users u
                    ON curr.user_key = u.user_key AND u.is_current = TRUE
            )
            SELECT
                date_key,
                platform_key,
                COUNT(DISTINCT CASE WHEN user_status = 'new' THEN user_key END)         AS new_users,
                COUNT(DISTINCT CASE WHEN user_status = 'retained' THEN user_key END)    AS retained,
                COUNT(DISTINCT CASE WHEN user_status = 'resurrected' THEN user_key END) AS resurrected,
                COUNT(DISTINCT user_key)                                                  AS total_dau
            FROM classified
            GROUP BY date_key, platform_key
            ORDER BY date_key, platform_key
        """).fetchdf()

    def get_quick_ratio(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Compute the Quick Ratio (new+resurrected / churned).

        Quick Ratio > 1 indicates growth; > 4 is excellent.
        """
        ga = self.get_growth_accounting(start_date, end_date)

        # Estimate churned: yesterday's DAU - today's retained
        ga = ga.sort_values(["platform_key", "date_key"])
        ga["prev_dau"] = ga.groupby("platform_key")["total_dau"].shift(1)
        ga["churned"] = (ga["prev_dau"] - ga["retained"]).clip(lower=0)
        ga["quick_ratio"] = ((ga["new_users"] + ga["resurrected"]) /
                              ga["churned"].replace(0, 1)).round(2)
        ga["net_growth"] = ga["new_users"] + ga["resurrected"] - ga["churned"]

        return ga

    def get_funnel_analysis(self, report_date: str) -> pd.DataFrame:
        """Engagement funnel: view → like → comment → share → create."""
        return self.conn.execute(f"""
            SELECT
                platform_key,
                COUNT(DISTINCT CASE WHEN event_type_key = 'content_view'
                                    THEN user_key END) AS viewers,
                COUNT(DISTINCT CASE WHEN event_type_key = 'like'
                                    THEN user_key END) AS likers,
                COUNT(DISTINCT CASE WHEN event_type_key = 'comment'
                                    THEN user_key END) AS commenters,
                COUNT(DISTINCT CASE WHEN event_type_key = 'share'
                                    THEN user_key END) AS sharers,
                COUNT(DISTINCT CASE WHEN event_type_key = 'content_create'
                                    THEN user_key END) AS creators,
                ROUND(COUNT(DISTINCT CASE WHEN event_type_key = 'like'
                    THEN user_key END) * 100.0 /
                    NULLIF(COUNT(DISTINCT CASE WHEN event_type_key = 'content_view'
                    THEN user_key END), 0), 2) AS view_to_like_pct,
                ROUND(COUNT(DISTINCT CASE WHEN event_type_key = 'content_create'
                    THEN user_key END) * 100.0 /
                    NULLIF(COUNT(DISTINCT CASE WHEN event_type_key = 'content_view'
                    THEN user_key END), 0), 2) AS view_to_create_pct
            FROM analytics.fct_events
            WHERE date_key = DATE '{report_date}'
            GROUP BY platform_key
            ORDER BY viewers DESC
        """).fetchdf()

    def get_geographic_growth(self) -> pd.DataFrame:
        """DAU and event growth by country."""
        return self.conn.execute("""
            WITH country_daily AS (
                SELECT
                    country,
                    date_key,
                    COUNT(DISTINCT user_key) AS dau,
                    COUNT(*) AS events
                FROM analytics.fct_events
                GROUP BY country, date_key
            ),
            first_last AS (
                SELECT
                    country,
                    MIN(date_key) AS first_date,
                    MAX(date_key) AS last_date
                FROM country_daily
                GROUP BY country
            )
            SELECT
                fl.country,
                cd_first.dau AS first_period_dau,
                cd_last.dau AS last_period_dau,
                ROUND((cd_last.dau - cd_first.dau) * 100.0 /
                    NULLIF(cd_first.dau, 0), 2) AS dau_growth_pct,
                SUM(cd.events) AS total_events
            FROM first_last fl
            JOIN country_daily cd_first
              ON fl.country = cd_first.country AND fl.first_date = cd_first.date_key
            JOIN country_daily cd_last
              ON fl.country = cd_last.country AND fl.last_date = cd_last.date_key
            JOIN country_daily cd ON fl.country = cd.country
            GROUP BY fl.country, cd_first.dau, cd_last.dau
            ORDER BY total_events DESC
        """).fetchdf()

    def get_demographic_breakdown(self, report_date: str) -> pd.DataFrame:
        """DAU breakdown by age group and device type."""
        return self.conn.execute(f"""
            SELECT
                u.age_group,
                u.device_type,
                COUNT(DISTINCT f.user_key) AS dau,
                COUNT(*) AS events,
                ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT f.user_key), 1) AS events_per_user
            FROM analytics.fct_events f
            JOIN analytics.dim_users u ON f.user_key = u.user_key AND u.is_current = TRUE
            WHERE f.date_key = DATE '{report_date}'
            GROUP BY u.age_group, u.device_type
            ORDER BY dau DESC
        """).fetchdf()
