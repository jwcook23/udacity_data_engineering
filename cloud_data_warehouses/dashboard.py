import os
import webbrowser
import psycopg2
import pandas as pd
import altair as alt

conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
cur = conn.cursor()

# free vs paid 
def get_level():
    query = """
    WITH _total AS (
        SELECT
            level,
            COUNT(*) AS _count
        FROM users
        GROUP BY level
    ) 
    SELECT
        level,
        _count / (SELECT SUM(_count) FROM _total) AS percent
    FROM _total;
    """

    data = pd.read_sql(query, conn)

    chart = alt.Chart(data, title='Service Level')
    chart = chart.mark_bar()
    chart = chart.encode(
        x=alt.X('percent:Q', stack='zero', axis=alt.Axis(format='%')),
        y=alt.Y('level:N')
    )

    text = alt.Chart(data).mark_text(dx=-15, dy=3, color='white')
    text = text.encode(
        x=alt.X('percent:Q', stack='zero'),
        y=alt.Y('level:N'),
        text=alt.Text('percent:Q', format='.0%')
    )

    return chart + text

def get_timeline():
    query = """
    SELECT
        time.day,
        COUNT(songplays.songplay_id) AS count
    FROM songplays
    LEFT JOIN time
        ON time.start_time = songplays.start_time
    GROUP BY day
    """
    data = pd.read_sql(query, conn)

    chart = alt.Chart(data)
    chart = chart.mark_line()
    chart = chart.encode(
        x='day:Q',
        y='count:Q'
    )

    return chart

# final dashboard
level = get_level()
level = level.properties(height=100)

timeline = get_timeline()
timeline = timeline.properties(height=200)

dashboard = level | timeline

# open dashboard in webbrowser
url = os.path.join(os.getcwd(),'dashboard.html')
dashboard.save(url)
webbrowser.open(os.path.join('file:' + url))