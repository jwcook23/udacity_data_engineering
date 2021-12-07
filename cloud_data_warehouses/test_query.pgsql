-- number of sessions before upgrading
-- number of plays before updating
-- churn rate: paid to free

-- TODO: document this query in steps for user 16
-- TODO: change MAX(level) & MAX(leve_current) to use FIRST_VALUE
SELECT
    user_id,
    level_group,
    MAX(level_current) AS level_current,
    MAX(level_previous) AS level_previous,
    MAX(user_session)-MIN(user_session) AS level_sessions,
    SUM(play_count) AS play_count,
    DATE_PART('day',MAX(start_time)-MIN(start_time)) AS level_time
FROM(
    -- level group
    SELECT
        user_id,
        level_current,
        level_previous,
        user_session,
        play_count,
        SUM(level_change) OVER (
            PARTITION BY user_id
            ORDER BY start_time ASC
        ) AS level_group,
        start_time
    FROM (
        -- level change
        SELECT 
            user_id,
            level_current,
            level_previous,
            CASE
                WHEN level_current<>level_previous THEN 1
                ELSE 0
            END AS level_change,
            user_session,
            play_count,
            start_time
        FROM (
            -- previous session
            SELECT
                user_id,
                level AS level_current,
                LAG (level,1) OVER (
                    PARTITION BY user_id
                    ORDER BY start_time ASC
                ) AS level_previous,
                RANK () OVER (
                    PARTITION BY user_id
                    ORDER BY start_time ASC
                ) AS user_session,
                play_count,
                start_time
            FROM (
                -- user session
                SELECT
                    session_id,
                    user_id,
                    MAX(level) AS level,
                    MAX(start_time) AS start_time,
                    COUNT(songplay_id) AS play_count
                FROM
                    songplays
                GROUP BY
                    user_id,
                    session_id
            ) AS _user_session
        ) AS _previous_session
    ) AS _level_change
) AS _level_group
GROUP BY user_id, level_group
ORDER BY user_id ASC
-- WHERE user_id=16