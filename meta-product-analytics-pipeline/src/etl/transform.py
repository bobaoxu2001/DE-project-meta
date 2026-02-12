"""
Transform Layer
================
Cleans, validates, and transforms raw data into the dimensional model.
Implements:
  - Data cleansing (deduplication, null handling, type casting)
  - Surrogate key generation
  - SCD Type 2 user dimension management
  - Fact table construction
  - Aggregate table computation
"""

import hashlib
import logging
from datetime import datetime

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class Transformer:
    """Transforms raw data into warehouse-ready dimensional structures."""

    @staticmethod
    def clean_events(events_df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate raw event data.

        Steps:
            1. Remove exact duplicates
            2. Handle null event_ids
            3. Cast timestamps
            4. Validate event types
            5. Remove future-dated events
        """
        initial_count = len(events_df)
        logger.info("Cleaning %d raw events...", initial_count)

        # 1. Remove exact duplicates
        events_df = events_df.drop_duplicates(subset=["event_id"])
        dupes_removed = initial_count - len(events_df)
        if dupes_removed > 0:
            logger.warning("Removed %d duplicate events", dupes_removed)

        # 2. Drop rows with null event_id or user_id
        events_df = events_df.dropna(subset=["event_id", "user_id"])

        # 3. Ensure timestamp is datetime
        events_df["event_timestamp"] = pd.to_datetime(
            events_df["event_timestamp"], errors="coerce"
        )
        events_df = events_df.dropna(subset=["event_timestamp"])

        # 4. Validate event types
        valid_events = {
            "app_open", "content_view", "content_create", "like",
            "comment", "share", "message_sent", "story_view",
            "story_create", "ad_impression", "ad_click", "search",
            "profile_view", "notification_open", "session_end",
        }
        events_df = events_df[events_df["event_type"].isin(valid_events)]

        # 5. Remove future-dated events
        now = datetime.utcnow()
        events_df = events_df[events_df["event_timestamp"] <= now]

        final_count = len(events_df)
        logger.info(
            "Cleaning complete: %d → %d events (%d removed)",
            initial_count, final_count, initial_count - final_count,
        )
        return events_df.reset_index(drop=True)

    @staticmethod
    def build_user_dimension(users_df: pd.DataFrame) -> pd.DataFrame:
        """Transform raw user data into SCD Type 2 dimension format.

        Generates surrogate keys and adds SCD metadata columns.
        """
        logger.info("Building user dimension from %d users...", len(users_df))

        dim = users_df.copy()

        # Generate surrogate key (user_key)
        dim["user_key"] = dim["user_id"].apply(
            lambda uid: hashlib.md5(uid.encode()).hexdigest()[:12]
        )

        # Ensure signup_date is date type
        dim["signup_date"] = pd.to_datetime(dim["signup_date"]).dt.date

        # SCD-2 metadata
        dim["effective_from"] = dim["signup_date"]
        dim["effective_to"] = None
        dim["is_current"] = True

        columns = [
            "user_key", "user_id", "country", "age_group", "device_type",
            "user_segment", "signup_date", "primary_platform",
            "effective_from", "effective_to", "is_current",
        ]
        dim = dim[columns]

        logger.info("User dimension built: %d rows", len(dim))
        return dim

    @staticmethod
    def build_fact_events(
        events_df: pd.DataFrame,
        user_dim: pd.DataFrame,
    ) -> pd.DataFrame:
        """Transform cleaned events into the fact table format.

        Resolves surrogate keys for user, platform, and event type dimensions.
        """
        logger.info("Building fact table from %d events...", len(events_df))

        fact = events_df.copy()

        # Resolve user surrogate key
        user_lookup = user_dim.set_index("user_id")["user_key"].to_dict()
        fact["user_key"] = fact["user_id"].map(user_lookup)

        # Drop events for unknown users
        unknown_users = fact["user_key"].isna().sum()
        if unknown_users > 0:
            logger.warning("Dropping %d events for unknown users", unknown_users)
            fact = fact.dropna(subset=["user_key"])

        # Platform key = platform name (natural key)
        fact["platform_key"] = fact["platform"]

        # Event type key = event type name (natural key)
        fact["event_type_key"] = fact["event_type"]

        # Date key
        fact["date_key"] = pd.to_datetime(fact["event_timestamp"]).dt.date

        # Partition date
        fact["_partition_date"] = fact["date_key"]

        # Event count (always 1 for raw events)
        fact["event_count"] = 1

        # Select final columns
        columns = [
            "event_id", "event_timestamp", "date_key", "user_key",
            "platform_key", "event_type_key", "session_id",
            "country", "device_type", "event_count", "_partition_date",
        ]
        fact = fact[columns]

        logger.info("Fact table built: %d rows", len(fact))
        return fact

    @staticmethod
    def compute_daily_aggregates(
        fact_df: pd.DataFrame,
        user_dim: pd.DataFrame,
    ) -> pd.DataFrame:
        """Compute pre-aggregated daily metrics per platform."""
        logger.info("Computing daily aggregates...")

        # Merge signup_date from user dim
        user_signup = user_dim[["user_key", "signup_date"]].copy()
        merged = fact_df.merge(user_signup, on="user_key", how="left")

        results = []
        for (date_key, platform_key), group in merged.groupby(
            ["date_key", "platform_key"]
        ):
            agg = {
                "date_key": date_key,
                "platform_key": platform_key,
                "dau": group["user_key"].nunique(),
                "new_users": group[
                    group["signup_date"] == date_key
                ]["user_key"].nunique(),
                "total_events": len(group),
                "total_sessions": group["session_id"].nunique(),
                "content_creates": (group["event_type_key"] == "content_create").sum(),
                "likes": (group["event_type_key"] == "like").sum(),
                "comments": (group["event_type_key"] == "comment").sum(),
                "shares": (group["event_type_key"] == "share").sum(),
                "messages_sent": (group["event_type_key"] == "message_sent").sum(),
                "ad_impressions": (group["event_type_key"] == "ad_impression").sum(),
                "ad_clicks": (group["event_type_key"] == "ad_click").sum(),
            }
            n_sessions = agg["total_sessions"]
            agg["avg_session_events"] = round(
                agg["total_events"] / n_sessions if n_sessions > 0 else 0, 2
            )
            results.append(agg)

        agg_df = pd.DataFrame(results)
        logger.info("Daily aggregates computed: %d rows", len(agg_df))
        return agg_df

    @staticmethod
    def compute_engagement_scores(
        fact_df: pd.DataFrame,
        report_date: str,
    ) -> pd.DataFrame:
        """Compute per-user engagement scores for a given report date.

        Metrics include L1/L7/L28 activity windows, cross-platform usage,
        and a composite engagement score (0-100).
        """
        from datetime import timedelta

        report_dt = pd.to_datetime(report_date).date()
        logger.info("Computing engagement scores for %s...", report_dt)

        # Date windows
        d1_start = report_dt
        d7_start = report_dt - timedelta(days=6)
        d28_start = report_dt - timedelta(days=27)

        fact_df = fact_df.copy()
        fact_df["date_key"] = pd.to_datetime(fact_df["date_key"]).apply(
            lambda x: x.date() if hasattr(x, "date") else x
        )

        # Filter to last 28 days
        window_df = fact_df[
            (fact_df["date_key"] >= d28_start) & (fact_df["date_key"] <= report_dt)
        ]

        if window_df.empty:
            logger.warning("No events in 28-day window for %s", report_dt)
            return pd.DataFrame()

        user_metrics = []
        for user_key, user_events in window_df.groupby("user_key"):
            active_dates = set(user_events["date_key"])

            l1 = report_dt in active_dates
            l7_dates = {d for d in active_dates if d >= d7_start}
            l28_dates = active_dates

            l7_events = user_events[user_events["date_key"] >= d7_start]
            platforms_l7 = l7_events["platform_key"].nunique()

            # Composite engagement score
            # Weighted: recency(30%) + frequency(30%) + breadth(20%) + volume(20%)
            recency_score = 100 if l1 else max(0, 100 - (report_dt - max(active_dates)).days * 10)
            frequency_score = min(100, len(l7_dates) / 7 * 100)
            breadth_score = min(100, platforms_l7 / 5 * 100)
            volume_score = min(100, len(l7_events) / 50 * 100)

            engagement_score = round(
                recency_score * 0.30
                + frequency_score * 0.30
                + breadth_score * 0.20
                + volume_score * 0.20,
                1,
            )

            user_metrics.append({
                "user_key": user_key,
                "date_key": report_dt,
                "l1_active": l1,
                "l7_active": len(l7_dates) > 0,
                "l28_active": len(l28_dates) > 0,
                "l7_days_active": len(l7_dates),
                "l28_days_active": len(l28_dates),
                "total_events_l7": len(l7_events),
                "total_events_l28": len(user_events),
                "platforms_used_l7": platforms_l7,
                "engagement_score": engagement_score,
            })

        result = pd.DataFrame(user_metrics)
        logger.info("Engagement scores computed for %d users", len(result))
        return result
