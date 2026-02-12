-- =============================================================================
-- Product Analytics SQL Queries
-- =============================================================================
-- Battle-tested analytical queries for social media product analytics.
-- Demonstrates advanced SQL: window functions, CTEs, cohort analysis,
-- funnel analysis, and cross-platform metrics.
-- =============================================================================


-- ---------------------------------------------------------------------------
-- 1. Daily Active Users (DAU) trend with 7-day moving average
-- ---------------------------------------------------------------------------
SELECT
    date_key,
    dau,
    ROUND(AVG(dau) OVER (
        ORDER BY date_key
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 0)                                                   AS dau_7d_ma,
    ROUND(AVG(dau) OVER (
        ORDER BY date_key
        ROWS BETWEEN 27 PRECEDING AND CURRENT ROW
    ), 0)                                                   AS dau_28d_ma,
    LAG(dau, 7)  OVER (ORDER BY date_key)                   AS dau_wow,
    ROUND((dau - LAG(dau, 7) OVER (ORDER BY date_key)) * 100.0
        / NULLIF(LAG(dau, 7) OVER (ORDER BY date_key), 0), 2) AS wow_growth_pct
FROM (
    SELECT
        date_key,
        SUM(dau) AS dau
    FROM analytics.agg_daily_metrics
    GROUP BY date_key
) daily
ORDER BY date_key;


-- ---------------------------------------------------------------------------
-- 2. DAU/MAU ratio (stickiness) — a key Meta metric
-- ---------------------------------------------------------------------------
WITH daily_users AS (
    SELECT
        f.date_key,
        COUNT(DISTINCT f.user_key) AS dau
    FROM analytics.fct_events f
    JOIN analytics.dim_event_type et ON f.event_type_key = et.event_type_key
    WHERE et.is_active_event = TRUE
    GROUP BY f.date_key
),
monthly_users AS (
    SELECT
        d.date_key,
        COUNT(DISTINCT f.user_key) AS mau
    FROM analytics.dim_date d
    JOIN analytics.fct_events f
      ON f.date_key BETWEEN d.date_key - INTERVAL '27 days' AND d.date_key
    JOIN analytics.dim_event_type et ON f.event_type_key = et.event_type_key
    WHERE et.is_active_event = TRUE
    GROUP BY d.date_key
)
SELECT
    du.date_key,
    du.dau,
    mu.mau,
    ROUND(du.dau * 100.0 / NULLIF(mu.mau, 0), 2) AS dau_mau_ratio_pct
FROM daily_users du
JOIN monthly_users mu ON du.date_key = mu.date_key
ORDER BY du.date_key;


-- ---------------------------------------------------------------------------
-- 3. Cross-platform usage analysis
-- ---------------------------------------------------------------------------
WITH user_platforms AS (
    SELECT
        user_key,
        date_key,
        COUNT(DISTINCT platform_key) AS platforms_used,
        ARRAY_AGG(DISTINCT platform_key ORDER BY platform_key) AS platform_list
    FROM analytics.fct_events
    GROUP BY user_key, date_key
)
SELECT
    platforms_used,
    COUNT(DISTINCT user_key)    AS num_users,
    ROUND(COUNT(DISTINCT user_key) * 100.0 /
        SUM(COUNT(DISTINCT user_key)) OVER (), 2) AS pct_of_users
FROM user_platforms
GROUP BY platforms_used
ORDER BY platforms_used;


-- ---------------------------------------------------------------------------
-- 4. Engagement funnel analysis (view → like → comment → share)
-- ---------------------------------------------------------------------------
WITH funnel AS (
    SELECT
        f.date_key,
        COUNT(DISTINCT CASE WHEN f.event_type_key = 'content_view'
                            THEN f.user_key END)    AS viewers,
        COUNT(DISTINCT CASE WHEN f.event_type_key = 'like'
                            THEN f.user_key END)    AS likers,
        COUNT(DISTINCT CASE WHEN f.event_type_key = 'comment'
                            THEN f.user_key END)    AS commenters,
        COUNT(DISTINCT CASE WHEN f.event_type_key = 'share'
                            THEN f.user_key END)    AS sharers,
        COUNT(DISTINCT CASE WHEN f.event_type_key = 'content_create'
                            THEN f.user_key END)    AS creators
    FROM analytics.fct_events f
    GROUP BY f.date_key
)
SELECT
    date_key,
    viewers,
    likers,
    commenters,
    sharers,
    creators,
    ROUND(likers     * 100.0 / NULLIF(viewers, 0), 2) AS view_to_like_pct,
    ROUND(commenters * 100.0 / NULLIF(viewers, 0), 2) AS view_to_comment_pct,
    ROUND(sharers    * 100.0 / NULLIF(viewers, 0), 2) AS view_to_share_pct,
    ROUND(creators   * 100.0 / NULLIF(viewers, 0), 2) AS view_to_create_pct
FROM funnel
ORDER BY date_key;


-- ---------------------------------------------------------------------------
-- 5. Power user concentration (Pareto analysis)
-- ---------------------------------------------------------------------------
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
    END                                         AS user_tier,
    COUNT(*)                                    AS num_users,
    SUM(total_events)                           AS total_events,
    ROUND(SUM(total_events) * 100.0 /
        (SELECT SUM(total_events) FROM user_events), 2)
                                                AS pct_of_total_events
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
ORDER BY MIN(pct_rank);


-- ---------------------------------------------------------------------------
-- 6. New-user activation analysis (D1, D7, D14, D30 retention)
-- ---------------------------------------------------------------------------
WITH new_users AS (
    SELECT
        user_key,
        signup_date AS cohort_date
    FROM analytics.dim_users
    WHERE is_current = TRUE
),
activity AS (
    SELECT DISTINCT user_key, date_key
    FROM analytics.fct_events
)
SELECT
    DATE_TRUNC('week', nu.cohort_date) AS cohort_week,
    COUNT(DISTINCT nu.user_key) AS cohort_size,
    COUNT(DISTINCT CASE
        WHEN a.date_key = nu.cohort_date + INTERVAL '1 day'
        THEN nu.user_key END)   AS retained_d1,
    COUNT(DISTINCT CASE
        WHEN a.date_key = nu.cohort_date + INTERVAL '7 days'
        THEN nu.user_key END)   AS retained_d7,
    COUNT(DISTINCT CASE
        WHEN a.date_key = nu.cohort_date + INTERVAL '14 days'
        THEN nu.user_key END)   AS retained_d14,
    COUNT(DISTINCT CASE
        WHEN a.date_key = nu.cohort_date + INTERVAL '30 days'
        THEN nu.user_key END)   AS retained_d30,
    ROUND(COUNT(DISTINCT CASE WHEN a.date_key = nu.cohort_date + INTERVAL '1 day'
        THEN nu.user_key END) * 100.0 / COUNT(DISTINCT nu.user_key), 2) AS d1_retention_pct,
    ROUND(COUNT(DISTINCT CASE WHEN a.date_key = nu.cohort_date + INTERVAL '7 days'
        THEN nu.user_key END) * 100.0 / COUNT(DISTINCT nu.user_key), 2) AS d7_retention_pct
FROM new_users nu
LEFT JOIN activity a ON nu.user_key = a.user_key
GROUP BY cohort_week
ORDER BY cohort_week;


-- ---------------------------------------------------------------------------
-- 7. Platform-level growth accounting (new, retained, resurrected, churned)
-- ---------------------------------------------------------------------------
WITH daily_active AS (
    SELECT DISTINCT user_key, date_key, platform_key
    FROM analytics.fct_events
),
classified AS (
    SELECT
        curr.date_key,
        curr.platform_key,
        curr.user_key,
        CASE
            WHEN prev.user_key IS NOT NULL                     THEN 'retained'
            WHEN u.signup_date = curr.date_key                 THEN 'new'
            WHEN prev.user_key IS NULL AND u.signup_date < curr.date_key THEN 'resurrected'
        END AS user_status
    FROM daily_active curr
    LEFT JOIN daily_active prev
        ON curr.user_key = prev.user_key
       AND curr.platform_key = prev.platform_key
       AND prev.date_key = curr.date_key - INTERVAL '1 day'
    JOIN analytics.dim_users u ON curr.user_key = u.user_key AND u.is_current = TRUE
)
SELECT
    date_key,
    platform_key,
    COUNT(DISTINCT CASE WHEN user_status = 'new' THEN user_key END)         AS new_users,
    COUNT(DISTINCT CASE WHEN user_status = 'retained' THEN user_key END)    AS retained_users,
    COUNT(DISTINCT CASE WHEN user_status = 'resurrected' THEN user_key END) AS resurrected_users,
    COUNT(DISTINCT user_key)                                                  AS total_dau
FROM classified
GROUP BY date_key, platform_key
ORDER BY date_key, platform_key;
