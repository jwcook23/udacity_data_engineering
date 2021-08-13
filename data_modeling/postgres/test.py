"""Performs a simple test to select the song_id/artist_id in songplays that is not null.

These were found using song and artist name from the log files queried against the songs and artists SQL tables.

"""

import psycopg2

conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
cur = conn.cursor()

cur.execute('SELECT song_id, artist_id FROM songplays WHERE song_id IS NOT NULL')
result = cur.fetchall()
print(result)