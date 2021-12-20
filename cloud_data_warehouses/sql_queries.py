'''Create syntax to drop/create tables, copy from S3 buckets to staging Redshift tables,
and insert data into the final analytical fact and dimension tables. Syntax is returned
in dictionaries to allow steps being performed during ETL to be transparent.'''

import configparser
from functools import partial
from collections import OrderedDict

# Load Configuration
config = configparser.ConfigParser()
config.read('dwh.cfg')

def drop_syntax():
    '''Generage drop table syntax.
    
    Parameters
    ----------
    None

    Returns
    -------
    drop (dict) : keys = table name, values = drop table syntax

    '''

    # drop tables using IF EXISTS clause to help facilitate testing
    tables = ['staging_events','staging_songs','songplay','users','songs','artists','time']
    drop = {t:'DROP TABLE IF EXISTS {table}' for t in tables}

    return drop

def create_syntax():
    ''' Generate syntax to create tables.

    Parameters
    ----------
    None

    Returns
    -------
    create (OrderedDict) : keys = table name, values = create table syntax
    '''

    # use an ordered dictionary as foreign keys require the reference table to be created first
    create = OrderedDict()

    # staging tables in a raw format without any validation checks
    create['staging_events'] = """
        CREATE TABLE {table} (
            artist VARCHAR(256),
            auth VARCHAR(256),
            firstName VARCHAR(256),
            gender VARCHAR(1),
            itemInSession SMALLINT,
            lastName VARCHAR(256),
            length DOUBLE PRECISION,
            level VARCHAR(4),
            location VARCHAR(256),
            method VARCHAR(3),
            page VARCHAR(256),
            registration BIGINT,
            sessionId BIGINT,
            song VARCHAR(256),
            status SMALLINT,
            ts BIGINT,
            userAgent VARCHAR(256),
            userId BIGINT
        )
    """
    create['staging_songs'] = """
        CREATE TABLE {table} (
            song_id VARCHAR(18),
            artist_id VARCHAR(18),
            title VARCHAR(256),
            year SMALLINT,
            duration DOUBLE PRECISION,
            artist_name VARCHAR(256),
            artist_location VARCHAR(256),
            artist_latitude DOUBLE PRECISION,
            artist_longitude DOUBLE PRECISION
        )
    """

    ## dimension tables
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

    ## fact table, using primary and foreign key constraints for the query optimizer
    create['songplay'] = """
        CREATE TABLE {table} (
            songplay_id BIGINT IDENTITY(0,1),
            song_id VARCHAR(18),
            artist_id VARCHAR(18),
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

    return create

def copy_syntax():
    '''Generate syntax for copy data from S3 buckets to Redshift staging tables.
    
    Parameters
    ----------
    None

    Returns
    -------
    copy (dict) : keys = tuple of (table_name, bucket_url), values = copy syntax
    '''

    copy = {}
    
    table = 'staging_events'
    bucket = config['S3']['LOG_DATA']
    query = [
    "COPY {table} FROM {bucket}",
    "CREDENTIALS 'aws_iam_role={iam_role}'",
    "COMPUPDATE OFF",                           # disable auto compression to improve loading many small files
    "FORMAT AS JSON {json_mapping}",            # prevent 'Delimiter not found' errors, possibly from multine JSON
    "MAXERROR 10"                               # skip errors such as String length exceeds DDL length
    ]
    query = '\n'.join(query)
    # add parameters except table and bucket that will be added during ETL for tracability
    copy[(table, bucket)] = partial(
        query.format,
        iam_role = config['IAM_ROLE']['ARN'],
        json_mapping=config['S3']['LOG_JSONPATH']   
    )

    table = 'staging_songs'
    bucket = config['S3']['SONG_DATA']
    query = [
    "COPY {table} FROM {bucket}",
    "CREDENTIALS 'aws_iam_role={iam_role}'",
    "COMPUPDATE OFF",                           # disable auto compression to improve loading many small files
    "FORMAT JSON 'auto'",                       # load data by directly using the JSON keys and values
    "MAXERROR 10",                              # skip errors such as String length exceeds DDL length
    ]
    query = '\n'.join(query)
    # add parameters except table and bucket that will be added during ETL for tracability
    copy[(table, bucket)] = partial(
        query.format,
        iam_role = config['IAM_ROLE']['ARN']
    )

    return copy

def insert_syntax():
    '''Generate syntax for insert data into fact and dimension tables from staging tables.

    Parameters
    ----------
    None

    Returns
    -------
    insert (dict) : keys = table name, values = insert syntax
    '''

    insert = {}

    # LEFT JOIN may produce NULL song_id and artist_id, but questions on user activity can still be answered
    # NextSong designates an event associated with a song play
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
    FROM staging_events AS logs
    LEFT JOIN staging_songs AS songs
        ON songs.artist_name = logs.artist
        AND songs.title = logs.song
        AND songs.duration = logs.length
    WHERE logs.page = 'NextSong'
    """)

    # unique user data by last played song (ex: level may change from "free" to "paid")
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

    # unique song data (once a song is released, other columns would remain static)
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

    # unique artist data for each artist_id (ex: artist_location may change)
    # records are deduplicated using the latest time values
    # A. first use latest year from staging_songs
    # B. for staging_songs.year ties, latest ts from staging events
    # C. if tie exists using both staging_songs.year and staging_events.ts, pick a record non-deterministically
    # non-deterministically ex: for multiple records year is 2001 and ts is NULL, ROW_NUMBER picks randomly
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

    # unique time datafor each song play
    # NextSong designates an event associated with a song play
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
        WHERE 
            page = 'NextSong'
    )
    """)

    return insert