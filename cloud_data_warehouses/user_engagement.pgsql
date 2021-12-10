-- Aggregations by user_id for each subscription level
-- Return
-- -------
-- user_id: identifier for each user
-- user_level_index: increases for previous subscription levels, current level = 1
-- level: subscription level
-- level_sessions: sessions at level
-- play_count: plays at level
-- level_days: time user spent at level, according to song plays

-- #TODO: move to ETL process to prevent complicated end user query

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

-- SELECT * FROM _session_summary ORDER BY start_time ASC
-- SELECT * FROM _session_previous
-- SELECT * FROM _level_change
-- SELECT * FROM _level_id
-- SELECT * FROM _group_level

-- aggreate user statistics by user level change
SELECT
    user_id,
    RANK() OVER (
        PARTITION BY user_id
        ORDER BY user_level_id DESC
    ) AS user_level_index,
    MAX(level_current) AS level,
    MAX(user_session_id)-MIN(user_session_id)+1 AS level_sessions,
    SUM(play_count) AS play_count,
    DATE_PART('day',MAX(start_time)-MIN(start_time)) AS level_days
FROM
    _group_level
GROUP BY 
    user_id, user_level_id