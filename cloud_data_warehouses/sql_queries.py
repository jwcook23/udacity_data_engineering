import configparser
from functools import partial
from collections import OrderedDict

# Load Configuration
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Drop Tables
## 1. IF EXISTS clause is used to help support testing purposes.
tables = ['staging_events','staging_songs','songplay','users','songs','artists','time']
drop = {t:'DROP TABLE IF EXISTS {table}' for t in tables}

# Create Tables
## 1. data is loaded into staging tables using raw column names and data types without any transformation or data validity checks
## 2. unknown length character columns use the default Redshift length of 256.
## 5. primary and foreign keys are defined in the fact and dimension tables for query optimization. Since Redshift does not 
## enforce them, they are later checked to make sure they remain valid.
create = OrderedDict()

create['staging_events'] = """
    CREATE TABLE {table} (
        artist VARCHAR(256),
        auth VARCHAR(256) NOT NULL,
        firstName VARCHAR(256),
        gender VARCHAR(1),
        itemInSession SMALLINT NOT NULL,
        lastName VARCHAR(256),
        length DOUBLE PRECISION,
        level VARCHAR(4) NOT NULL,
        location VARCHAR(256),
        method VARCHAR(3) NOT NULL,
        page VARCHAR(256) NOT NULL,
        registration BIGINT,
        sessionId BIGINT NOT NULL,
        song VARCHAR(256),
        status SMALLINT NOT NULL,
        ts BIGINT NOT NULL,
        userAgent VARCHAR(256),
        userId BIGINT
    )
"""


create['staging_songs'] = """
    CREATE TABLE {table} (
        song_id VARCHAR(18) NOT NULL,
        artist_id VARCHAR(18) NOT NULL,
        title VARCHAR(256) NOT NULL,
        year SMALLINT,
        duration DOUBLE PRECISION NOT NULL,
        artist_name VARCHAR(256) NOT NULL,
        artist_location VARCHAR(256),
        artist_latitude DOUBLE PRECISION,
        artist_longitude DOUBLE PRECISION
    )
"""

## dimension table 1
create['users'] = """
    CREATE TABLE {table} (
        user_id BIGINT NOT NULL,
        first_name VARCHAR(256) NOT NULL,
        last_name VARCHAR(256) NOT NULL,
        gender VARCHAR(1) NOT NULL,
        level VARCHAR(4) NOT NULL,
        PRIMARY KEY (user_id)
    )
"""
## dimension table 2
create['songs'] = """
    CREATE TABLE {table} (
        song_id VARCHAR(18) NOT NULL,
        title VARCHAR(256) NOT NULL,
        artist_id VARCHAR(18) NOT NULL,
        year SMALLINT,
        duration DOUBLE PRECISION NOT NULL,
        PRIMARY KEY (song_id)
    )
"""
## dimension table 3
create['artists'] = """
    CREATE TABLE {table} (
        artist_id VARCHAR(18) NOT NULL,
        artist_name VARCHAR(256) NOT NULL,
        artist_location VARCHAR(256),
        artist_latitude DOUBLE PRECISION,
        artist_longitude DOUBLE PRECISION,
        PRIMARY KEY (artist_id)
    )
"""
## dimension table 4
create['time'] = """
    CREATE TABLE {table} (
        start_time TIMESTAMP NOT NULL SORTKEY,
        hour SMALLINT NOT NULL,
        day SMALLINT NOT NULL,
        week SMALLINT NOT NULL,
        month SMALLINT NOT NULL,
        year SMALLINT NOT NULL,
        weekday SMALLINT NOT NULL,
        PRIMARY KEY (start_time)
    )
"""

## fact table
create['songplay'] = """
    CREATE TABLE {table} (
        songplay_id BIGINT IDENTITY(0,1),
        song_id VARCHAR(18) NOT NULL,
        artist_id VARCHAR(18) NOT NULL,
        start_time TIMESTAMP NOT NULL SORTKEY,
        user_id BIGINT NOT NULL,
        session_id BIGINT NOT NULL,
        level VARCHAR(4) NOT NULL,
        location VARCHAR(256),
        user_agent VARCHAR(256) NOT NULL,
        PRIMARY KEY (songplay_id),
        FOREIGN KEY (user_id) REFERENCES users(user_id),
        FOREIGN KEY (song_id) REFERENCES songs(song_id),
        FOREIGN KEY (artist_id) REFERENCES artists(artist_id),
        FOREIGN KEY (start_time) REFERENCES time(start_time)
    )
"""

# Copy from AWS S3 into Redshift Staging Tables
## COMPUPDATE OFF: disables automatic compression to improve performance when loading many smaller files
## TIMEFORMAT: not set as 'epochmillisecs', instead
## FORMAT AS JSON: could optionally specify a subset of columns to load since all might not be used
## TODO: why is FORMAT AS JSON different in the first example?
## ideally data would be loaded for new files in S3 using a lambda trigger, but only a one time ELT is being done for this project

copy = OrderedDict()

table = 'staging_events'
s3 = config['S3']['LOG_DATA']
query = ("""
COPY {table} FROM {s3}
CREDENTIALS 'aws_iam_role={iam_role}'
COMPUPDATE OFF region '{region}'
FORMAT AS JSON {json_mapping};
""")
copy[(table, s3)] = partial(
    query.format,
    iam_role = config['IAM_ROLE']['ARN'],
    region=config['AWS']['REGION'],
    json_mapping=config['S3']['LOG_JSONPATH']   
)

table = 'staging_songs'
s3 = config['S3']['SONG_DATA']
query = ("""
COPY {table} FROM {s3}
CREDENTIALS 'aws_iam_role={iam_role}'
COMPUPDATE OFF region '{region}'
FORMAT JSON 'auto';
""")
copy[(table, s3)] = partial(
    query.format,
    iam_role = config['IAM_ROLE']['ARN'],
    region=config['AWS']['REGION']
)

# Insert from Redshift Staging Tables into Final Redshift Tables
## ideally the data would be loaded from the staging tables into the fact & diminsion tables using an UPSERT technique
## this is not done as only a one time ETL is being performed
insert = OrderedDict()

# combination of song and events data 
insert['songplay'] = ("""
INSERT INTO {table} (
    start_time, 
    user_id, 
    level, 
    song_id, 
    artist_id, 
    session_id, 
    location, 
    user_agent
)
SELECT
    (TIMESTAMP 'epoch' + logs.ts/1000 * INTERVAL '1 Second ') AS start_time,
    logs.userId AS user_id,
    logs.level,
    songs.song_id,
    songs.artist_id,
    logs.sessionId AS session_id,
    logs.location,
    logs.userAgent AS user_agent
FROM staging_songs AS songs
INNER JOIN staging_events AS logs
    ON songs.artist_name = logs.artist
    AND songs.title = logs.song
    AND songs.duration = logs.length
WHERE logs.page = 'Next Song'
""")

# unique user data by last played song for each user_id
## example: level may change from "free" to "paid"
insert['users'] = ("""
INSERT INTO {table}
SELECT
    user_id,
    first_name,
    last_name,
    gender,
    level
FROM (
    SELECT
        userId AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender,
        level,
        RANK() OVER (
            PARTITION BY user_id
            ORDER BY ts DESC NULLS LAST
        ) AS _latest        
    FROM
        staging_events
    WHERE
        user_id IS NOT NULL
) WHERE _latest = 1
""")

# unique song data for each song_id
## once a song is released, columns in this table would remain static
insert['songs'] = ("""
INSERT INTO {table}
SELECT 
    song_id,
    title,
    artist_id,
    year,
    duration
FROM (
    SELECT
        song_id,
        MAX(title) AS title,
        MAX(artist_id) AS artist_id,
        MAX(year) AS year,
        MAX(duration) as duration
    FROM
        staging_songs
    GROUP BY song_id
)
""")

# unique artist data for each artist_id by year of song
## last played song as an attempted tie-breaker, if a tie does occur a record is picked non-deterministically
## artist location and lat/long may be updated
insert['artists'] = ("""
INSERT INTO {table}
SELECT
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM (
    SELECT
        staging_songs.artist_id,
        staging_songs.artist_name,
        staging_songs.artist_location,
        staging_songs.artist_latitude,
        staging_songs.artist_longitude,
        ROW_NUMBER() OVER (
            PARTITION BY staging_songs.artist_id
            ORDER BY staging_songs.year DESC, staging_events.ts DESC
        ) AS _latest
    FROM staging_songs
    LEFT JOIN staging_events
        ON staging_songs.artist_name = staging_events.artist
        AND staging_songs.title = staging_events.song
        AND staging_songs.duration = staging_events.length
)
WHERE _latest = 1
""")

insert['time'] = ("""
INSERT INTO {table} (
    start_time, 
    hour, 
    day, 
    week, 
    month, 
    year, 
    weekday
)
SELECT 
    _timestamp, 
    EXTRACT(HOUR FROM _timestamp), 
    EXTRACT(DAY FROM _timestamp), 
    EXTRACT(WEEK FROM _timestamp), 
    EXTRACT(MONTH FROM _timestamp), 
    EXTRACT(YEAR FROM _timestamp), 
    EXTRACT(WEEKDAY FROM _timestamp)
FROM ( 
    SELECT DISTINCT
        (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second ') as _timestamp
    FROM 
        staging_events
)
""")