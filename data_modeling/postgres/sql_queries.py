""" Defines the SQL statements to create, drop, select, and insert into SQL tables."""

# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays"
user_table_drop = "DROP TABLE IF EXISTS dim_users"
song_table_drop = "DROP TABLE IF EXISTS dim_songs"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES

## fact table: songplay
## primary key is auto-generated on the SQL side
songplay_table_create = ("""
CREATE TABLE songplays(
songplay_id SERIAL PRIMARY KEY,
start_time TIMESTAMP NOT NULL,
user_id SMALLINT NOT NULL,
level VARCHAR NOT NULL,
song_id VARCHAR,
artist_id VARCHAR,
session_id SMALLINT NOT NULL,
location VARCHAR NOT NULL, 
user_agent VARCHAR NOT NULL,
CONSTRAINT user_id FOREIGN KEY (user_id) REFERENCES users(user_id),
CONSTRAINT song_id FOREIGN KEY (song_id) REFERENCES songs(song_id),
CONSTRAINT artist_id FOREIGN KEY (artist_id) REFERENCES artists(artist_id),
CONSTRAINT start_time FOREIGN KEY (start_time) REFERENCES time(start_time)
)
""")

## dimension table: users
user_table_create = ("""
CREATE TABLE users(
user_id SMALLINT PRIMARY KEY, 
first_name VARCHAR NOT NULL, 
last_name VARCHAR NOT NULL, 
gender VARCHAR NOT NULL, 
level VARCHAR NOT NULL
)
""")

## dimension table: songs
song_table_create = ("""
CREATE TABLE songs(
song_id VARCHAR PRIMARY KEY,
title VARCHAR NOT NULL,
artist_id VARCHAR NOT NULL,
year SMALLINT NOT NULL, 
duration DOUBLE PRECISION NOT NULL
)
""")

## dimension table: artists
artist_table_create = ("""
CREATE TABLE artists(
artist_id VARCHAR PRIMARY KEY, 
name VARCHAR NOT NULL, 
location VARCHAR NOT NULL, 
latitude DOUBLE PRECISION, 
longitude DOUBLE PRECISION
)
""")

## dimension table: time
time_table_create = ("""
CREATE TABLE time(
start_time TIMESTAMP PRIMARY KEY, 
hour SMALLINT NOT NULL, 
day SMALLINT NOT NULL, 
week SMALLINT NOT NULL,
month SMALLINT NOT NULL,
year SMALLINT NOT NULL, 
weekday SMALLINT NOT NULL
)
""")

# INSERT RECORDS

## fact table: songplay
songplay_table_insert = ("""
INSERT INTO songplays
(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

## dimension table: users
### update the level in the event a user changes between paid/free service
user_table_insert = ("""
INSERT INTO users
(user_id, first_name, last_name, gender, level)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level
""")

## dimension table: songs
song_table_insert = ("""
INSERT INTO songs
(song_id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id)
DO NOTHING;
""")

## dimension table: artists
artist_table_insert = ("""
INSERT INTO artists
(artist_id, name, location, latitude, longitude)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id)
DO NOTHING;
""")

## dimension table: time
time_table_insert = ("""
INSERT INTO time
(start_time, hour, day, week, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time)
DO NOTHING;
""")

# FIND SONGS
# # determine song_id and artist_id as logs only contains song & artist name
song_select = ("""
SELECT songs.song_id, artists.artist_id FROM songs 
JOIN artists ON (songs.artist_id = songs.artist_id)
WHERE songs.title = %s AND artists.name = %s AND duration = %s
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [user_table_drop, song_table_drop, artist_table_drop, time_table_drop, songplay_table_drop]