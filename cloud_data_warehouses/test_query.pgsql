-- target: number of sessions before upgrading
-- target: number of plays before upgrading
-- target: length of time before upgrading
-- KPI: churn rate: paid to free

-- TODO: document this query in steps for user 16
-- TODO: change MAX(level) & MAX(leve_current) to use FIRST_VALUE


WITH 
-- session summary for each user
_session_summary AS (
    SELECT
        session_id,
        user_id,
        MIN(level) AS level,
        MIN(start_time) AS start_time,
        COUNT(songplay_id) AS play_count
    FROM
        songplays
    -- WHERE user_id=16
    GROUP BY
        user_id,
        session_id
),
-- calculate previous level and session index for user
_session_previous AS (
    SELECT
        user_id,
        RANK () OVER (
            PARTITION BY user_id
            ORDER BY start_time ASC
        ) AS user_session_id,
        level AS level_current,
        LAG (level,1) OVER (
            PARTITION BY user_id
            ORDER BY start_time ASC
        ) AS level_previous,
        play_count,
        start_time
    FROM
        _session_summary
),
-- determine if level changed between sessions user
_level_change AS (
    SELECT 
        user_id,
        user_session_id,
        level_current,
        level_previous,
        CASE
            WHEN level_current<>level_previous THEN 1
            ELSE 0
        END AS level_change,
        play_count,
        start_time
    FROM
        _session_previous
),
-- assign levels a group ID for user
_level_id AS (
    SELECT
        user_id,
        user_session_id,
        SUM(level_change) OVER (
            PARTITION BY user_id
            ORDER BY start_time ASC
        ) AS user_level_id,
        level_current,
        level_previous,
        play_count,
        start_time
    FROM
        _level_change
),
-- determine level_current and level_previous for a user session group
_group_level AS (
    SELECT
        user_id,
        user_level_id,
        FIRST_VALUE (level_current) OVER (
            PARTITION BY user_id, user_level_id
            ORDER BY start_time ASC
        ) AS level_current,
        FIRST_VALUE (level_previous) OVER (
            PARTITION BY user_id, user_level_id
            ORDER BY start_time ASC
        ) AS level_previous,
        user_session_id,
        play_count,
        start_time
    FROM
        _level_id
)

-- SELECT * FROM _session_summary
-- SELECT * FROM _session_previous
-- SELECT * FROM _level_change
-- SELECT * FROM _level_id
-- SELECT * FROM _group_level

-- aggreate user statistics by user level change
SELECT
    user_id,
    user_level_id,
    MAX(level_current) AS level_current,
    MAX(level_previous) AS level_previous,
    MAX(user_session_id)-MIN(user_session_id) AS level_sessions,
    SUM(play_count) AS play_count,
    DATE_PART('day',MAX(start_time)-MIN(start_time)) AS level_time
FROM
    _group_level
GROUP BY 
    user_id, user_level_id