-- Aggregations by user_id for each subscription level
-- Return
-- -------
-- user_id: identifier for each user
-- user_level_index: increases for previous subscription levels, current level = 1
-- level_current: subscription level
-- level_previous: level of previous subscription level with null identifying first user level
-- level_next: level of next subscription level with null identifying last user level
-- level_sessions: sessions at level
-- play_count: plays at level
-- level_days: time user spent at level, according to song plays

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
        songplay
    -- WHERE user_id=15
    GROUP BY
        user_id,
        session_id
),
-- calculate previous level and session index for user
_session_comparison AS (
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
        LEAD (level,1) OVER (
            PARTITION BY user_id
            ORDER BY start_time ASC        
        ) AS level_next,
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
        level_next,
        CASE
            WHEN level_current<>level_previous THEN 1
            ELSE 0
        END AS level_change,
        play_count,
        start_time
    FROM
        _session_comparison
),
-- assign levels a group ID for user
_level_id AS (
    SELECT
        user_id,
        user_session_id,
        SUM(level_change) OVER (
            PARTITION BY user_id
            ORDER BY start_time ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS user_level_id,
        level_current,
        level_previous,
        level_next,
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
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS level_current,
        FIRST_VALUE (level_previous) OVER (
            PARTITION BY user_id, user_level_id
            ORDER BY start_time ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS level_previous,
        FIRST_VALUE (level_next) OVER (
            PARTITION BY user_id, user_level_id
            ORDER BY start_time DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS level_next,
        user_session_id,
        play_count,
        start_time
    FROM
        _level_id
)

-- SELECT user_id, session_id, level, TO_CHAR(start_time, 'YYYY-MM-DD') AS start_time, play_count FROM _session_summary ORDER BY start_time ASC
-- SELECT * FROM _session_comparison
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
    MAX(level_current) AS level_current,
    MAX(level_previous) AS level_previous,
    MAX(level_next) AS level_next,
    MAX(user_session_id)-MIN(user_session_id)+1 AS level_sessions,
    SUM(play_count) AS play_count,
    -- Postgres
    -- DATE_PART('day',MAX(start_time)-MIN(start_time)) AS level_days
    -- Redshift
    DATEDIFF('day',MIN(start_time),MAX(start_time)) AS level_days
FROM
    _group_level
GROUP BY 
    user_id, user_level_id