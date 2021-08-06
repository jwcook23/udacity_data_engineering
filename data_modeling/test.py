import psycopg2


conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
cur = conn.cursor()

cur.execute('SELECT song_id, artist_id FROM songplays WHERE song_id IS NOT NULL')
result = cur.fetchall()
print(result)