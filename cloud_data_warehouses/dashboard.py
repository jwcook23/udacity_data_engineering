import os
import webbrowser
import psycopg2
import pandas as pd
import altair as alt
from vega_datasets import data

conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
cur = conn.cursor()

# free vs paid 
def level():
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
        x=alt.X('percent', stack='zero', axis=alt.Axis(format='%')),
        y=alt.Y('level')
    )
    chart = chart.properties(height=100)

    text = alt.Chart(data).mark_text(dx=-15, dy=3, color='white')
    text = text.encode(
        x=alt.X('percent', stack='zero'),
        y=alt.Y('level'),
        text=alt.Text('percent', format='.0%')
    )

    return chart + text

def datetrend():
    # TODO: verify ON time.start_time = songplays.start_time JOIN
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

    chart = alt.Chart(data, title = 'Song Play Date Trend')
    chart = chart.mark_line()
    chart = chart.encode(
        x='day',
        y='count'
    )
    chart = chart.properties(height=200)

    return chart

def hourbucket():
    # TODO: verify ON time.start_time = songplays.start_time JOIN
    query = """
    SELECT
        time.hour,
        COUNT(songplays.songplay_id) AS count
    FROM songplays
    LEFT JOIN time
        ON time.start_time = songplays.start_time
    GROUP BY time.hour
    """
    data = pd.read_sql(query, conn)

    chart = alt.Chart(data, title='Song Play Hourly Distribution')
    chart = chart.mark_bar()
    chart = chart.encode(
        x=alt.X('hour', bin=alt.BinParams(step=1), axis=alt.Axis(format='.0f')),
        y='count'
    )
    chart = chart.properties(height=150)

    return chart

def location():

    # derive state from location extrafter comma, first dash
    # extract state from text after common then before dash
    # ex: Minneapolis-St. Paul-Bloomington, MN-WI -> MN
    query = """
    WITH _parsed AS (
        SELECT
            TRIM(
                SPLIT_PART(
                    SPLIT_PART(location, ',', 2),
                    '-',
                    1
                )
            ) AS abbr
        FROM songplays
    )
    SELECT 
        abbr,
        COUNT(abbr) AS play_count
    FROM _parsed
    GROUP BY abbr
    """
    plays = pd.read_sql(query, conn)
    states = alt.topo_feature(data.us_10m.url, feature='states')

    # get ANSI standard id field with abbr to match to us_10m dataset
    ansi = pd.read_csv('https://www2.census.gov/geo/docs/reference/state.txt', sep='|')
    ansi.columns = ['id', 'abbr', 'state', 'statens']
    ansi = ansi[['id', 'abbr', 'state']]
    ansi = ansi.merge(plays, left_on='abbr', right_on='abbr', how='right')

    base = alt.Chart(states).mark_geoshape(fill='lightgray', stroke='black', strokeWidth=0.5)

    chart = alt.Chart(states).mark_geoshape(stroke='black').encode(
        color='play_count:Q'
    ).transform_lookup(
        lookup='id',
        from_=alt.LookupData(ansi, 'id', ['play_count'])
    ).properties(
        width=500,
        height=300
    ).project('albersUsa')

    return base + chart

# position each chart into final dashboard
# TODO: associated played songs (because you also listed to)
# TODO: trending songs, trending artists
# TODO: text over heatmap artist country? https://altair-viz.github.io/gallery/layered_heatmap_text.html
dashboard = location() #level() | (hourbucket() & datetrend())

# open dashboard in webbrowser
url = os.path.join(os.getcwd(),'dashboard.html')
dashboard.save(url)
webbrowser.open(os.path.join('file:' + url))