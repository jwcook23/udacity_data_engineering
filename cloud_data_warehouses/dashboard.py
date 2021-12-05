import os
import webbrowser
from altair.vegalite.v4.schema.core import TitleParams
import psycopg2
import pandas as pd
import altair as alt
from vega_datasets import data as vega_data

conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
cur = conn.cursor()

def dashboard_title():
    '''Create empty graph to use as the overall dashboard title.'''
    chart = alt.Chart(
        pd.DataFrame({'x': [None], 'y':[None]}), 
        title=alt.TitleParams('User Play Dashboard',fontSize=20)
    ).mark_bar().encode(
        x=alt.X('x', axis=None),
        y=alt.Y('y', axis=None),
        opacity=alt.value(0)
    )
    return chart

# free vs paid 
def user_subscription():
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

    source = pd.read_sql(query, conn)

    chart = alt.Chart(source, title='User Subscription Level')
    chart = chart.mark_bar()
    chart = chart.encode(
        x=alt.X('percent', stack='zero', axis=alt.Axis(format='%'), title='Percent'),
        y=alt.Y('level', title='Level'),
        color='level'
    )

    text = alt.Chart(source).mark_text(dx=-15, dy=3, color='white')
    text = text.encode(
        x=alt.X('percent', stack='zero'),
        y=alt.Y('level'),
        text=alt.Text('percent', format='.0%')
    )

    return chart + text

def play_trend():
    # TODO: verify ON time.start_time = songplays.start_time JOIN
    query = """
    SELECT
        time.day,
        MIN(weekday) AS weekday,
        COUNT(songplays.songplay_id) AS count
    FROM songplays
    LEFT JOIN time
        ON time.start_time = songplays.start_time
    GROUP BY day
    """
    source = pd.read_sql(query, conn)

    chart = alt.Chart(source, title = 'Song Play Date Trend')
    chart = chart.mark_line()
    chart = chart.encode(
        x=alt.X('day', title='Day'),
        y=alt.X('count', title='Play Count'),
        color=alt.value('black')
    )

    # classify weekend as starting on saturday
    weekend = source[source['weekday']==6].copy()
    weekend['Time of Week']='Weekend'
    weekend['stop'] = weekend['day']+2
    rect = alt.Chart(weekend).mark_rect().encode(
        x='day',
        x2='stop',
        opacity=alt.value(0.4),
        color='Time of Week:N'
        # color=alt.value('red')
    )

    return chart + rect

def play_distribution():
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
    source = pd.read_sql(query, conn)

    chart = alt.Chart(source, title='Song Play Hourly Distribution')
    chart = chart.mark_bar()
    chart = chart.encode(
        x=alt.X('hour', bin=alt.BinParams(step=1), axis=alt.Axis(format='.0f'), title='Hour'),
        y=alt.X('count', title='Play Count')
    )

    return chart

def play_location():

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
    source = pd.read_sql(query, conn)
    states = alt.topo_feature(vega_data.us_10m.url, feature='states')

    # get ANSI standard ID field with abbr to match to us_10m dataset
    # state_id = pd.read_csv('https://www2.census.gov/geo/docs/reference/state.txt', sep='|')
    # state_id.columns = ['id', 'abbr', 'state', 'statens']
    # state_id = state_id[['id', 'abbr', 'state']]
    # state_id.to_csv('state_id.csv', index=False)
    state_id = pd.read_csv('state_id.csv')

    # add play count to ANSI standard ID
    state_id = state_id.merge(source, left_on='abbr', right_on='abbr', how='right')

    # create base map of US states to plot states with no play_count
    base = alt.Chart(states, title='Song Plays by State')
    base = base.mark_geoshape(fill='white', stroke='black', strokeWidth=0.5)

    # create outlines of states with heatmap of play_count
    chart = alt.Chart(states)
    chart = chart.mark_geoshape(stroke='black')
    chart = chart.encode(
        color=alt.Color('play_count:Q', title='Play Count', scale=alt.Scale(scheme='lightmulti'))
    )
    chart = chart.transform_lookup(
        lookup='id',
        from_=alt.LookupData(state_id, 'id', ['play_count'])
    )
    # add appropriate zoom/view (projection)
    chart = chart.project('albersUsa')

    return base + chart

def user_top25pct():

    query = """
    SELECT
        user_id,
        level,
        playcount
    FROM (
        WITH _count AS(
            SELECT 
                songplays.user_id,
                COUNT(songplay_id) AS playcount,
                MAX(users.level) AS level
            FROM songplays
            INNER JOIN users
                ON users.user_id = songplays.user_id
            GROUP BY songplays.user_id
            ORDER BY playcount DESC
        )
        SELECT
            user_id,
            level,
            playcount,
            PERCENT_RANK() OVER (
                ORDER BY playcount
            ) AS _rank
        FROM _count
    ) AS _percentile
    WHERE _percentile._rank>=0.75
    ORDER BY playcount DESC
    """
    source = pd.read_sql(query, conn)

    chart = alt.Chart(source, title='Top 25% of Users')
    chart = chart.mark_bar()
    chart = chart.encode(
        x=alt.X('user_id:N', sort='-y', title='User ID', axis=alt.Axis(labelAngle=-45)),
        y=alt.Y('playcount:Q', title='Play Count'),
        color='level'
    )

    return chart

# position each chart into final dashboard
dashboard = alt.hconcat(
    # column 1
    alt.vconcat(
        alt.hconcat(
            alt.vconcat(
                dashboard_title().properties(width=200, height=1),
                user_subscription().properties(width=200, height=100), 
            ),
            user_top25pct().properties(width=300, height=150)
        ),
        play_distribution().properties(width=400, height=180),
        play_trend().properties(width=400, height=100)
    ).resolve_scale(color='independent'),
    # column 2
        play_location().properties(width=450, height=250)
    # TODO: something with session id
    # TODO: something with user agent
)

# open dashboard in webbrowser
url = os.path.join(os.getcwd(),'dashboard.html')
dashboard.save(url)
webbrowser.open(os.path.join('file:' + url))