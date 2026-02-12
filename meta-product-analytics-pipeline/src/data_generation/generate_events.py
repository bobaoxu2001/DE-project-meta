"""
Social Media Event Data Generator
===================================
Generates realistic synthetic event data simulating user interactions
across a family of social media applications (Facebook, Instagram,
Messenger, WhatsApp, Threads) at scale.

Produces raw event logs with realistic distributions for:
- User demographics and segments
- Platform usage patterns (cross-platform behavior)
- Temporal patterns (time-of-day, day-of-week seasonality)
- Engagement funnels (power users vs. casual users)
"""

import os
import logging
import hashlib
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PLATFORMS = ["facebook", "instagram", "messenger", "whatsapp", "threads"]

EVENT_TYPES = [
    "app_open", "content_view", "content_create", "like", "comment",
    "share", "message_sent", "story_view", "story_create", "ad_impression",
    "ad_click", "search", "profile_view", "notification_open", "session_end",
]

# Relative probability weights for each event type (models a realistic funnel)
EVENT_WEIGHTS = {
    "app_open": 0.18,
    "content_view": 0.25,
    "content_create": 0.03,
    "like": 0.12,
    "comment": 0.04,
    "share": 0.03,
    "message_sent": 0.10,
    "story_view": 0.07,
    "story_create": 0.01,
    "ad_impression": 0.08,
    "ad_click": 0.01,
    "search": 0.03,
    "profile_view": 0.03,
    "notification_open": 0.02,
    "session_end": 0.00,  # Derived from app_open
}

# Platform usage probability (some users prefer certain platforms)
PLATFORM_WEIGHTS = {
    "facebook": 0.30,
    "instagram": 0.30,
    "messenger": 0.15,
    "whatsapp": 0.15,
    "threads": 0.10,
}

COUNTRIES = [
    ("US", 0.25), ("IN", 0.18), ("BR", 0.10), ("ID", 0.07),
    ("MX", 0.05), ("PH", 0.04), ("VN", 0.04), ("TH", 0.03),
    ("GB", 0.03), ("DE", 0.03), ("FR", 0.03), ("JP", 0.03),
    ("KR", 0.02), ("CA", 0.02), ("AU", 0.02), ("OTHER", 0.06),
]

AGE_GROUPS = ["13-17", "18-24", "25-34", "35-44", "45-54", "55-64", "65+"]
AGE_WEIGHTS = [0.08, 0.25, 0.30, 0.18, 0.10, 0.06, 0.03]

DEVICE_TYPES = ["ios", "android", "web", "tablet"]
DEVICE_WEIGHTS = [0.35, 0.45, 0.15, 0.05]


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _generate_user_id(index: int) -> str:
    """Generate a deterministic, anonymized user ID."""
    return hashlib.sha256(f"user_{index}".encode()).hexdigest()[:16]


def _hour_weight(hour: int) -> float:
    """Return a weight reflecting typical social-media usage by hour (UTC)."""
    weights = [
        0.02, 0.01, 0.01, 0.01, 0.01, 0.02,  # 0-5   (low)
        0.03, 0.05, 0.06, 0.06, 0.06, 0.06,   # 6-11  (morning ramp)
        0.07, 0.07, 0.06, 0.05, 0.05, 0.06,   # 12-17 (afternoon)
        0.07, 0.08, 0.07, 0.05, 0.04, 0.03,   # 18-23 (evening peak)
    ]
    return weights[hour]


def _day_of_week_weight(dow: int) -> float:
    """Return weight by day-of-week (0=Monday). Weekends are higher."""
    weights = [0.13, 0.13, 0.13, 0.13, 0.14, 0.17, 0.17]
    return weights[dow]


# ---------------------------------------------------------------------------
# User dimension generator
# ---------------------------------------------------------------------------

class UserGenerator:
    """Generates a synthetic user dimension table."""

    def __init__(self, num_users: int, seed: int = 42):
        self.num_users = num_users
        self.rng = np.random.default_rng(seed)

    def generate(self) -> pd.DataFrame:
        """Return a DataFrame of synthetic user profiles."""
        logger.info("Generating %d user profiles...", self.num_users)

        user_ids = [_generate_user_id(i) for i in range(self.num_users)]

        # Country distribution
        country_codes, country_probs = zip(*COUNTRIES)
        countries = self.rng.choice(
            list(country_codes), size=self.num_users, p=list(country_probs)
        )

        # Age group distribution
        age_groups = self.rng.choice(
            AGE_GROUPS, size=self.num_users, p=AGE_WEIGHTS
        )

        # Device type
        devices = self.rng.choice(
            DEVICE_TYPES, size=self.num_users, p=DEVICE_WEIGHTS
        )

        # User segment (power / active / casual / dormant)
        segments = self.rng.choice(
            ["power", "active", "casual", "dormant"],
            size=self.num_users,
            p=[0.05, 0.25, 0.50, 0.20],
        )

        # Account creation date (spread over last 5 years)
        base_date = datetime(2021, 1, 1)
        days_offsets = self.rng.integers(0, 5 * 365, size=self.num_users)
        signup_dates = [base_date + timedelta(days=int(d)) for d in days_offsets]

        # Primary platform preference per user
        platform_prefs = self.rng.choice(
            PLATFORMS, size=self.num_users,
            p=list(PLATFORM_WEIGHTS.values()),
        )

        df = pd.DataFrame({
            "user_id": user_ids,
            "country": countries,
            "age_group": age_groups,
            "device_type": devices,
            "user_segment": segments,
            "signup_date": signup_dates,
            "primary_platform": platform_prefs,
        })

        logger.info("User profiles generated: %d rows", len(df))
        return df


# ---------------------------------------------------------------------------
# Event generator
# ---------------------------------------------------------------------------

class EventGenerator:
    """Generates synthetic event streams for social media product analytics."""

    SEGMENT_ACTIVITY = {
        "power": (20, 50),
        "active": (5, 20),
        "casual": (1, 5),
        "dormant": (0, 1),
    }

    def __init__(
        self,
        users_df: pd.DataFrame,
        start_date: str = "2025-11-01",
        num_days: int = 90,
        seed: int = 42,
    ):
        self.users_df = users_df
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        self.num_days = num_days
        self.rng = np.random.default_rng(seed)

        # Pre-compute event probability vector
        event_names = list(EVENT_WEIGHTS.keys())
        event_probs = np.array([EVENT_WEIGHTS[e] for e in event_names])
        event_probs = event_probs / event_probs.sum()
        self.event_names = event_names
        self.event_probs = event_probs

    def _events_for_day(self, date: datetime) -> pd.DataFrame:
        """Generate all events for a single day."""
        dow = date.weekday()
        dow_weight = _day_of_week_weight(dow)

        records = []
        for _, user in self.users_df.iterrows():
            segment = user["user_segment"]
            lo, hi = self.SEGMENT_ACTIVITY[segment]

            # Scale by day-of-week
            scaled_hi = int(hi * dow_weight / 0.14)
            n_events = self.rng.integers(lo, max(lo + 1, scaled_hi + 1))

            if n_events == 0:
                continue

            # Pick event types
            events = self.rng.choice(
                self.event_names, size=n_events, p=self.event_probs
            )

            # Pick hours weighted by usage pattern
            hours = self.rng.choice(
                24, size=n_events,
                p=[_hour_weight(h) for h in range(24)],
            )
            minutes = self.rng.integers(0, 60, size=n_events)
            seconds = self.rng.integers(0, 60, size=n_events)

            # Decide platform: 70 % primary, 30 % random
            platforms = []
            for _ in range(n_events):
                if self.rng.random() < 0.70:
                    platforms.append(user["primary_platform"])
                else:
                    platforms.append(self.rng.choice(PLATFORMS))

            for i in range(n_events):
                ts = date.replace(
                    hour=int(hours[i]),
                    minute=int(minutes[i]),
                    second=int(seconds[i]),
                )
                records.append({
                    "event_id": hashlib.md5(
                        f"{user['user_id']}_{ts.isoformat()}_{i}".encode()
                    ).hexdigest(),
                    "user_id": user["user_id"],
                    "event_type": events[i],
                    "platform": platforms[i],
                    "event_timestamp": ts,
                    "country": user["country"],
                    "device_type": user["device_type"],
                    "session_id": hashlib.md5(
                        f"{user['user_id']}_{date.strftime('%Y%m%d')}_{hours[i]}".encode()
                    ).hexdigest()[:12],
                })

        return pd.DataFrame(records)

    def generate(self, output_dir: str = "data/raw/events") -> str:
        """Generate events for the full date range and write Parquet files.

        Returns the output directory path.
        """
        os.makedirs(output_dir, exist_ok=True)
        total_events = 0

        for day_offset in range(self.num_days):
            current_date = self.start_date + timedelta(days=day_offset)
            date_str = current_date.strftime("%Y-%m-%d")

            logger.info("Generating events for %s ...", date_str)
            df = self._events_for_day(current_date)

            # Write as date-partitioned Parquet
            out_path = os.path.join(output_dir, f"dt={date_str}", "events.parquet")
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            table = pa.Table.from_pandas(df)
            pq.write_table(table, out_path, compression="snappy")

            total_events += len(df)
            logger.info("  -> %d events written to %s", len(df), out_path)

        logger.info("Total events generated: %d across %d days", total_events, self.num_days)
        return output_dir


# ---------------------------------------------------------------------------
# Quick-run generation (smaller dataset for demo / CI)
# ---------------------------------------------------------------------------

def generate_demo_dataset(
    num_users: int = 5000,
    num_days: int = 30,
    output_base: str = "data/raw",
    seed: int = 42,
) -> tuple[pd.DataFrame, str]:
    """Generate a small demo dataset suitable for local testing.

    Returns:
        (users_df, events_dir)
    """
    user_gen = UserGenerator(num_users=num_users, seed=seed)
    users_df = user_gen.generate()

    # Save users dimension
    users_path = os.path.join(output_base, "users", "users.parquet")
    os.makedirs(os.path.dirname(users_path), exist_ok=True)
    users_df.to_parquet(users_path, index=False)
    logger.info("Users saved to %s", users_path)

    # Generate events
    event_gen = EventGenerator(
        users_df=users_df,
        start_date="2025-11-01",
        num_days=num_days,
        seed=seed,
    )
    events_dir = event_gen.generate(
        output_dir=os.path.join(output_base, "events")
    )

    return users_df, events_dir


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    import argparse

    parser = argparse.ArgumentParser(description="Generate synthetic social media event data")
    parser.add_argument("--users", type=int, default=5000, help="Number of users")
    parser.add_argument("--days", type=int, default=30, help="Number of days")
    parser.add_argument("--output", type=str, default="data/raw", help="Output base directory")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    args = parser.parse_args()

    generate_demo_dataset(
        num_users=args.users,
        num_days=args.days,
        output_base=args.output,
        seed=args.seed,
    )
    print("Data generation complete.")
