-- =============================================================================
-- ETL Transformation Queries
-- =============================================================================
-- Reusable SQL transformations for the product analytics pipeline.
-- Target: DuckDB / ANSI SQL
-- =============================================================================


-- ---------------------------------------------------------------------------
-- 1. Populate dim_date (generate calendar for a date range)
-- ---------------------------------------------------------------------------
INSERT OR REPLACE INTO analytics.dim_date
SELECT
    date_key,
    EXTRACT(YEAR FROM date_key)                             AS year,
    EXTRACT(QUARTER FROM date_key)                          AS quarter,
    EXTRACT(MONTH FROM date_key)                            AS month,
    STRFTIME(date_key, '%B')                                AS month_name,
    EXTRACT(WEEK FROM date_key)                             AS week_of_year,
    EXTRACT(DAY FROM date_key)                              AS day_of_month,
    EXTRACT(DOW FROM date_key)                              AS day_of_week,
    STRFTIME(date_key, '%A')                                AS day_name,
    EXTRACT(DOW FROM date_key) IN (5, 6)                    AS is_weekend,
    CONCAT('FY', EXTRACT(YEAR FROM date_key) % 100 +
        CASE WHEN EXTRACT(MONTH FROM date_key) >= 10 THEN 1 ELSE 0 END,
        '-Q', EXTRACT(QUARTER FROM date_key))               AS fiscal_quarter
FROM generate_series(
    DATE '2025-01-01',
    DATE '2026-12-31',
    INTERVAL '1 day'
) AS t(date_key);


-- ---------------------------------------------------------------------------
-- 2. Populate dim_platform (static seed data)
-- ---------------------------------------------------------------------------
INSERT OR REPLACE INTO analytics.dim_platform VALUES
    ('facebook',  'Facebook',  'social'),
    ('instagram', 'Instagram', 'media'),
    ('messenger', 'Messenger', 'messaging'),
    ('whatsapp',  'WhatsApp',  'messaging'),
    ('threads',   'Threads',   'social');


-- ---------------------------------------------------------------------------
-- 3. Populate dim_event_type (static seed data)
-- ---------------------------------------------------------------------------
INSERT OR REPLACE INTO analytics.dim_event_type VALUES
    ('app_open',          'App Open',          'session',       TRUE),
    ('content_view',      'Content View',      'engagement',    TRUE),
    ('content_create',    'Content Create',    'content',       TRUE),
    ('like',              'Like',              'engagement',    TRUE),
    ('comment',           'Comment',           'engagement',    TRUE),
    ('share',             'Share',             'engagement',    TRUE),
    ('message_sent',      'Message Sent',      'engagement',    TRUE),
    ('story_view',        'Story View',        'engagement',    TRUE),
    ('story_create',      'Story Create',      'content',       TRUE),
    ('ad_impression',     'Ad Impression',     'monetization',  FALSE),
    ('ad_click',          'Ad Click',          'monetization',  TRUE),
    ('search',            'Search',            'engagement',    TRUE),
    ('profile_view',      'Profile View',      'engagement',    TRUE),
    ('notification_open', 'Notification Open', 'session',       TRUE),
    ('session_end',       'Session End',       'session',       FALSE);


-- ---------------------------------------------------------------------------
-- 4. Build agg_daily_metrics from fct_events
-- ---------------------------------------------------------------------------
INSERT OR REPLACE INTO analytics.agg_daily_metrics
SELECT
    f.date_key,
    f.platform_key,
    COUNT(DISTINCT f.user_key)                                              AS dau,
    COUNT(DISTINCT CASE WHEN u.signup_date = f.date_key
                        THEN f.user_key END)                                AS new_users,
    COUNT(*)                                                                AS total_events,
    COUNT(DISTINCT f.session_id)                                            AS total_sessions,
    COUNT(CASE WHEN f.event_type_key = 'content_create' THEN 1 END)        AS content_creates,
    COUNT(CASE WHEN f.event_type_key = 'like' THEN 1 END)                  AS likes,
    COUNT(CASE WHEN f.event_type_key = 'comment' THEN 1 END)               AS comments,
    COUNT(CASE WHEN f.event_type_key = 'share' THEN 1 END)                 AS shares,
    COUNT(CASE WHEN f.event_type_key = 'message_sent' THEN 1 END)          AS messages_sent,
    COUNT(CASE WHEN f.event_type_key = 'ad_impression' THEN 1 END)         AS ad_impressions,
    COUNT(CASE WHEN f.event_type_key = 'ad_click' THEN 1 END)              AS ad_clicks,
    ROUND(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT f.session_id), 0), 2)     AS avg_session_events
FROM analytics.fct_events f
JOIN analytics.dim_users u ON f.user_key = u.user_key
GROUP BY f.date_key, f.platform_key;


-- ---------------------------------------------------------------------------
-- 5. DAU / WAU / MAU query (cross-platform)
-- ---------------------------------------------------------------------------
-- This is a parameterized query template; replace :report_date with actual date.
-- DAU / WAU / MAU for a given date
/*
SELECT
    :report_date                                                            AS report_date,
    COUNT(DISTINCT CASE WHEN f.date_key = :report_date
                        THEN f.user_key END)                                AS dau,
    COUNT(DISTINCT CASE WHEN f.date_key BETWEEN :report_date - INTERVAL '6 days'
                                             AND :report_date
                        THEN f.user_key END)                                AS wau,
    COUNT(DISTINCT CASE WHEN f.date_key BETWEEN :report_date - INTERVAL '27 days'
                                             AND :report_date
                        THEN f.user_key END)                                AS mau
FROM analytics.fct_events f
JOIN analytics.dim_event_type et ON f.event_type_key = et.event_type_key
WHERE et.is_active_event = TRUE;
*/


-- ---------------------------------------------------------------------------
-- 6. Retention cohort builder
-- ---------------------------------------------------------------------------
INSERT OR REPLACE INTO analytics.agg_retention_cohorts
WITH user_cohorts AS (
    SELECT
        user_key,
        DATE_TRUNC('week', signup_date)  AS cohort_week,
        primary_platform                  AS platform_key
    FROM analytics.dim_users
    WHERE is_current = TRUE
),
user_activity AS (
    SELECT DISTINCT
        user_key,
        date_key
    FROM analytics.fct_events
),
retention AS (
    SELECT
        uc.cohort_week,
        uc.platform_key,
        FLOOR(DATEDIFF('day', uc.cohort_week, ua.date_key) / 7)  AS weeks_since_signup,
        COUNT(DISTINCT uc.user_key)                                AS retained_users
    FROM user_cohorts uc
    JOIN user_activity ua ON uc.user_key = ua.user_key
    WHERE ua.date_key >= uc.cohort_week
    GROUP BY uc.cohort_week, uc.platform_key, weeks_since_signup
),
cohort_sizes AS (
    SELECT
        cohort_week,
        platform_key,
        COUNT(DISTINCT user_key) AS cohort_size
    FROM user_cohorts
    GROUP BY cohort_week, platform_key
)
SELECT
    r.cohort_week,
    r.platform_key,
    r.weeks_since_signup,
    cs.cohort_size,
    r.retained_users,
    ROUND(r.retained_users * 1.0 / cs.cohort_size, 4) AS retention_rate
FROM retention r
JOIN cohort_sizes cs
  ON r.cohort_week = cs.cohort_week
 AND r.platform_key = cs.platform_key
ORDER BY r.cohort_week, r.platform_key, r.weeks_since_signup;
