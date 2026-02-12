"""
Extract Layer
==============
Reads raw event and user data from Parquet files (data lake).
Supports incremental extraction by date partition.
"""

import glob
import logging
import os

import pandas as pd
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


class Extractor:
    """Extracts raw data from the data lake (Parquet files)."""

    def __init__(self, raw_data_dir: str = "data/raw"):
        self.raw_data_dir = raw_data_dir

    def extract_users(self) -> pd.DataFrame:
        """Extract user dimension data."""
        users_path = os.path.join(self.raw_data_dir, "users", "users.parquet")
        if not os.path.exists(users_path):
            raise FileNotFoundError(f"Users file not found: {users_path}")

        df = pd.read_parquet(users_path)
        logger.info("Extracted %d user records from %s", len(df), users_path)
        return df

    def extract_events(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """Extract event data, optionally filtering by date range.

        Args:
            start_date: Inclusive start date (YYYY-MM-DD).
            end_date: Inclusive end date (YYYY-MM-DD).

        Returns:
            DataFrame of raw events.
        """
        events_dir = os.path.join(self.raw_data_dir, "events")
        pattern = os.path.join(events_dir, "dt=*", "events.parquet")
        files = sorted(glob.glob(pattern))

        if not files:
            raise FileNotFoundError(f"No event files found matching {pattern}")

        # Filter by date partition if specified
        if start_date or end_date:
            filtered = []
            for fp in files:
                # Extract date from partition path: .../dt=YYYY-MM-DD/events.parquet
                dir_name = os.path.basename(os.path.dirname(fp))
                date_str = dir_name.replace("dt=", "")
                if start_date and date_str < start_date:
                    continue
                if end_date and date_str > end_date:
                    continue
                filtered.append(fp)
            files = filtered

        logger.info("Reading %d event partition(s)...", len(files))

        dfs = []
        for fp in files:
            df = pd.read_parquet(fp)
            dfs.append(df)

        result = pd.concat(dfs, ignore_index=True)
        logger.info("Extracted %d total event records", len(result))
        return result

    def extract_events_for_date(self, date_str: str) -> pd.DataFrame:
        """Extract events for a single date partition."""
        return self.extract_events(start_date=date_str, end_date=date_str)

    def list_available_dates(self) -> list[str]:
        """Return sorted list of available date partitions."""
        events_dir = os.path.join(self.raw_data_dir, "events")
        pattern = os.path.join(events_dir, "dt=*")
        dirs = sorted(glob.glob(pattern))
        dates = [os.path.basename(d).replace("dt=", "") for d in dirs]
        logger.info("Found %d date partitions", len(dates))
        return dates
