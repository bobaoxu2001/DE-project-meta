"""
Product Analytics Dashboard
=============================
Interactive dashboard built with Plotly/Dash for visualizing
product analytics KPIs across the social media platform family.

Panels:
  1. DAU / WAU / MAU trend with stickiness ratio
  2. Platform comparison
  3. Engagement funnel
  4. Retention cohort heatmap
  5. Growth accounting
  6. Geographic breakdown
  7. Power user distribution
"""

import logging

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

logger = logging.getLogger(__name__)


class AnalyticsDashboard:
    """Generate interactive Plotly visualizations for product analytics."""

    COLORS = {
        "facebook": "#1877F2",
        "instagram": "#E4405F",
        "messenger": "#00B2FF",
        "whatsapp": "#25D366",
        "threads": "#000000",
    }

    THEME = "plotly_dark"

    def __init__(self, db_path: str = "data/warehouse/product_analytics.duckdb"):
        self.conn = duckdb.connect(db_path, read_only=True)

    def close(self):
        self.conn.close()

    # ------------------------------------------------------------------
    # 1. DAU Trend
    # ------------------------------------------------------------------

    def plot_dau_trend(self) -> go.Figure:
        """DAU trend with 7-day moving average."""
        df = self.conn.execute("""
            SELECT
                date_key,
                SUM(dau) AS dau,
                SUM(total_events) AS total_events
            FROM analytics.agg_daily_metrics
            GROUP BY date_key
            ORDER BY date_key
        """).fetchdf()

        df["dau_7d_ma"] = df["dau"].rolling(7, min_periods=1).mean()

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df["date_key"], y=df["dau"],
            name="DAU", marker_color="#1877F2", opacity=0.5,
        ))
        fig.add_trace(go.Scatter(
            x=df["date_key"], y=df["dau_7d_ma"],
            name="7-day MA", line=dict(color="#E4405F", width=3),
        ))
        fig.update_layout(
            title="Daily Active Users (DAU) Trend",
            xaxis_title="Date",
            yaxis_title="Users",
            template=self.THEME,
            hovermode="x unified",
            height=450,
        )
        return fig

    # ------------------------------------------------------------------
    # 2. Platform Comparison
    # ------------------------------------------------------------------

    def plot_platform_comparison(self) -> go.Figure:
        """Compare key metrics across platforms."""
        df = self.conn.execute("""
            SELECT
                platform_key,
                SUM(dau) / COUNT(DISTINCT date_key) AS avg_dau,
                SUM(total_events) AS total_events,
                SUM(content_creates) AS content_creates,
                SUM(likes + comments + shares) AS interactions
            FROM analytics.agg_daily_metrics
            GROUP BY platform_key
            ORDER BY avg_dau DESC
        """).fetchdf()

        fig = make_subplots(
            rows=1, cols=2,
            subplot_titles=("Average DAU by Platform", "Total Interactions"),
            specs=[[{"type": "bar"}, {"type": "pie"}]],
        )

        colors = [self.COLORS.get(p, "#666") for p in df["platform_key"]]

        fig.add_trace(go.Bar(
            x=df["platform_key"], y=df["avg_dau"],
            marker_color=colors, name="Avg DAU",
        ), row=1, col=1)

        fig.add_trace(go.Pie(
            labels=df["platform_key"],
            values=df["interactions"],
            marker_colors=colors,
            hole=0.4,
        ), row=1, col=2)

        fig.update_layout(
            title="Platform Performance Comparison",
            template=self.THEME,
            showlegend=False,
            height=400,
        )
        return fig

    # ------------------------------------------------------------------
    # 3. Engagement Funnel
    # ------------------------------------------------------------------

    def plot_engagement_funnel(self, report_date: str) -> go.Figure:
        """Funnel: viewers → likers → commenters → sharers → creators."""
        df = self.conn.execute(f"""
            SELECT
                COUNT(DISTINCT CASE WHEN event_type_key = 'content_view'
                                    THEN user_key END) AS viewers,
                COUNT(DISTINCT CASE WHEN event_type_key = 'like'
                                    THEN user_key END) AS likers,
                COUNT(DISTINCT CASE WHEN event_type_key = 'comment'
                                    THEN user_key END) AS commenters,
                COUNT(DISTINCT CASE WHEN event_type_key = 'share'
                                    THEN user_key END) AS sharers,
                COUNT(DISTINCT CASE WHEN event_type_key = 'content_create'
                                    THEN user_key END) AS creators
            FROM analytics.fct_events
            WHERE date_key = DATE '{report_date}'
        """).fetchone()

        stages = ["View Content", "Like", "Comment", "Share", "Create Content"]
        values = list(df)

        fig = go.Figure(go.Funnel(
            y=stages,
            x=values,
            textinfo="value+percent initial",
            marker=dict(color=["#1877F2", "#E4405F", "#25D366", "#00B2FF", "#FF6900"]),
        ))
        fig.update_layout(
            title=f"Engagement Funnel — {report_date}",
            template=self.THEME,
            height=400,
        )
        return fig

    # ------------------------------------------------------------------
    # 4. Retention Heatmap
    # ------------------------------------------------------------------

    def plot_retention_heatmap(self) -> go.Figure:
        """Weekly cohort retention heatmap."""
        df = self.conn.execute("""
            SELECT cohort_week, weeks_since_signup, retention_rate
            FROM analytics.agg_retention_cohorts
            WHERE platform_key = 'facebook'
            ORDER BY cohort_week, weeks_since_signup
        """).fetchdf()

        if df.empty:
            fig = go.Figure()
            fig.add_annotation(text="No retention data available", showarrow=False)
            return fig

        pivot = df.pivot_table(
            index="cohort_week", columns="weeks_since_signup",
            values="retention_rate", aggfunc="first",
        )

        fig = go.Figure(go.Heatmap(
            z=pivot.values * 100,
            x=[f"Week {int(c)}" for c in pivot.columns],
            y=[str(d)[:10] for d in pivot.index],
            colorscale="Blues",
            text=[[f"{v:.1f}%" if pd.notna(v) else "" for v in row]
                  for row in pivot.values * 100],
            texttemplate="%{text}",
            hovertemplate="Cohort: %{y}<br>Week: %{x}<br>Retention: %{z:.1f}%<extra></extra>",
        ))
        fig.update_layout(
            title="Weekly Cohort Retention (Facebook)",
            xaxis_title="Weeks Since Signup",
            yaxis_title="Cohort Week",
            template=self.THEME,
            height=500,
        )
        return fig

    # ------------------------------------------------------------------
    # 5. Growth Accounting
    # ------------------------------------------------------------------

    def plot_growth_accounting(self) -> go.Figure:
        """Stacked area chart showing new / retained / resurrected users."""
        df = self.conn.execute("""
            WITH daily_active AS (
                SELECT DISTINCT user_key, date_key
                FROM analytics.fct_events
            ),
            classified AS (
                SELECT
                    curr.date_key,
                    curr.user_key,
                    CASE
                        WHEN u.signup_date = curr.date_key THEN 'New'
                        WHEN prev.user_key IS NOT NULL THEN 'Retained'
                        ELSE 'Resurrected'
                    END AS user_type
                FROM daily_active curr
                LEFT JOIN daily_active prev
                    ON curr.user_key = prev.user_key
                   AND prev.date_key = curr.date_key - INTERVAL '1 day'
                JOIN analytics.dim_users u
                    ON curr.user_key = u.user_key AND u.is_current = TRUE
            )
            SELECT
                date_key,
                user_type,
                COUNT(DISTINCT user_key) AS users
            FROM classified
            GROUP BY date_key, user_type
            ORDER BY date_key
        """).fetchdf()

        fig = px.area(
            df, x="date_key", y="users", color="user_type",
            color_discrete_map={
                "New": "#25D366",
                "Retained": "#1877F2",
                "Resurrected": "#FF6900",
            },
        )
        fig.update_layout(
            title="Growth Accounting — DAU Composition",
            xaxis_title="Date",
            yaxis_title="Users",
            template=self.THEME,
            hovermode="x unified",
            height=450,
        )
        return fig

    # ------------------------------------------------------------------
    # 6. Geographic Distribution
    # ------------------------------------------------------------------

    def plot_geo_distribution(self) -> go.Figure:
        """World map showing DAU distribution by country."""
        df = self.conn.execute("""
            SELECT
                country,
                COUNT(DISTINCT user_key) AS total_users,
                COUNT(*) AS total_events
            FROM analytics.fct_events
            GROUP BY country
        """).fetchdf()

        fig = go.Figure(go.Choropleth(
            locations=df["country"],
            z=df["total_users"],
            locationmode="ISO-3",
            colorscale="Blues",
            colorbar_title="Users",
        ))
        fig.update_layout(
            title="User Distribution by Country",
            template=self.THEME,
            height=450,
            geo=dict(showframe=False, projection_type="natural earth"),
        )
        return fig

    # ------------------------------------------------------------------
    # 7. Power User Distribution
    # ------------------------------------------------------------------

    def plot_engagement_distribution(self) -> go.Figure:
        """Histogram of engagement scores with segment overlay."""
        df = self.conn.execute("""
            SELECT
                e.engagement_score,
                u.user_segment
            FROM analytics.agg_user_engagement e
            JOIN analytics.dim_users u ON e.user_key = u.user_key AND u.is_current = TRUE
        """).fetchdf()

        fig = px.histogram(
            df, x="engagement_score", color="user_segment",
            nbins=50, barmode="overlay", opacity=0.7,
            color_discrete_sequence=["#E4405F", "#1877F2", "#25D366", "#666"],
        )
        fig.update_layout(
            title="Engagement Score Distribution by User Segment",
            xaxis_title="Engagement Score (0-100)",
            yaxis_title="Number of Users",
            template=self.THEME,
            height=400,
        )
        return fig

    # ------------------------------------------------------------------
    # Export all charts
    # ------------------------------------------------------------------

    def generate_all_charts(
        self, report_date: str, output_dir: str = "data/processed/charts"
    ) -> list[str]:
        """Generate and save all dashboard charts as HTML files.

        Returns list of output file paths.
        """
        import os
        os.makedirs(output_dir, exist_ok=True)

        charts = {
            "01_dau_trend": self.plot_dau_trend(),
            "02_platform_comparison": self.plot_platform_comparison(),
            "03_engagement_funnel": self.plot_engagement_funnel(report_date),
            "04_retention_heatmap": self.plot_retention_heatmap(),
            "05_growth_accounting": self.plot_growth_accounting(),
            "06_geo_distribution": self.plot_geo_distribution(),
            "07_engagement_distribution": self.plot_engagement_distribution(),
        }

        output_paths = []
        for name, fig in charts.items():
            path = os.path.join(output_dir, f"{name}.html")
            fig.write_html(path, include_plotlyjs="cdn")
            output_paths.append(path)
            logger.info("Chart saved: %s", path)

        logger.info("All %d charts saved to %s", len(charts), output_dir)
        return output_paths


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)

    report_date = sys.argv[1] if len(sys.argv) > 1 else "2025-11-30"
    dashboard = AnalyticsDashboard()
    paths = dashboard.generate_all_charts(report_date)
    dashboard.close()
    print(f"Generated {len(paths)} charts")
