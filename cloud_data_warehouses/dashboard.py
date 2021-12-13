import os
import webbrowser
from altair.vegalite.v4.schema.channels import StrokeOpacity
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

    return chart.properties(width=200, height=1)

def user_level():
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
        x=alt.X('percent', stack='zero', axis=alt.Axis(format='%'), title='Percent of Users'),
        y=alt.Y('level', title='Level'),
        color='level'
    )

    text = alt.Chart(source).mark_text(dx=-15, dy=3, color='white')
    text = text.encode(
        x=alt.X('percent', stack='zero'),
        y=alt.Y('level'),
        text=alt.Text('percent', format='.0%')
    )

    return (chart + text).properties(width=250, height=100)

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
        y=alt.X('count', title='Plays'),
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
        color=alt.Color('Time of Week:N',legend=alt.Legend(orient='bottom'), title=None)
    )

    return (chart + rect).properties(width=300, height=100)

def play_level():
    query = """
    WITH _total AS (
        SELECT
            level,
            COUNT(*) AS _count
        FROM songplays
        GROUP BY level
    ) 
    SELECT
        level,
        _count / (SELECT SUM(_count) FROM _total) AS percent
    FROM _total;
    """

    source = pd.read_sql(query, conn)

    chart = alt.Chart(source, title='Play Subscription Level')
    chart = chart.mark_bar()
    chart = chart.encode(
        x=alt.X('percent', stack='zero', axis=alt.Axis(format='%'), title='Percent of Plays'),
        y=alt.Y('level', title='Level'),
        color=alt.Color('level', legend=None),
    )

    text = alt.Chart(source).mark_text(dx=-15, dy=3, color='white')
    text = text.encode(
        x=alt.X('percent', stack='zero'),
        y=alt.Y('level'),
        text=alt.Text('percent', format='.0%')
    )

    return (chart + text).properties(width=250, height=100)
    
def play_hour():
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
        x=alt.X('hour:Q', bin=alt.BinParams(step=1), axis=alt.Axis(format='.0f'), title='Hour'),
        y=alt.X('count', title='Plays')
    )

    return chart.properties(width=250, height=100)

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
        color=alt.Color('play_count:Q', title='Plays', scale=alt.Scale(scheme='lightmulti'))
    )
    chart = chart.transform_lookup(
        lookup='id',
        from_=alt.LookupData(state_id, 'id', ['play_count'])
    )
    # add appropriate zoom/view (projection)
    chart = chart.project('albersUsa')

    return (base + chart).properties(width=450, height=250)

def user_top25pct():

    # TODO: deduplicate users with multiple levels
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
        x=alt.X('user_id:N', sort='-y', title='User ID', axis=alt.Axis(labelAngle=60)),
        y=alt.Y('playcount:Q', title='Plays'),
        color=alt.Color('level', legend=alt.Legend(orient='bottom'))
    )

    return chart.properties(width=250, height=150)

def user_agent():

    # TODO: move case to ETL process
    query = """
    SELECT
        os,
        browser,
        COUNT(songplay_id) AS playcount
    FROM (
        SELECT
            songplay_id,
        CASE
            WHEN user_agent LIKE '%Windows%' THEN 'Windows'
            WHEN user_agent LIKE '%Linux%' THEN 'Linux'
            WHEN user_agent LIKE '%Macintosh%' THEN 'Mac'
            WHEN user_agent LIKE '%iPhone%' THEN 'iPhone'
        END AS os,
        CASE
            WHEN user_agent LIKE '%Chrome%' THEN 'Chrome'
            WHEN user_agent LIKE '%Firefox%' THEN 'Firefox'
            WHEN user_agent LIKE '%Trident%' THEN 'IE'
            WHEN user_agent LIKE '%Mobile%' THEN 'Mobile'
            WHEN user_agent LIKE '%Safari%' THEN 'Safari'
        END AS browser
        FROM songplays
    ) AS _agent
    GROUP BY _agent.os, _agent.browser
    """

    source = pd.read_sql(query, conn)

    base = alt.Chart(source, title='User Agent')
    base = base.encode(
        alt.X('os', scale=alt.Scale(paddingInner=0), title='Operating System', axis=alt.Axis(labelAngle=-45)),
        alt.Y('browser', scale=alt.Scale(paddingInner=0), title='Browser'),
    )

    heatmap = base.mark_rect().encode(
        color=alt.Color('playcount',
            scale=alt.Scale(scheme='viridis'), title='Plays'
        )
    )

    text = base.mark_text(baseline='middle').encode(
        text='playcount',
        color=alt.condition(
            alt.datum.playcount > 100,
            alt.value('black'),
            alt.value('white')
        )
    )

    return (heatmap + text).properties(width=175, height=100)

def user_comparison(user_levels):

    #TODO: sessions length boxplot by level
    #TODO: sessions per visit boxplot by level

    # boxplot: session count by level
    # boxplot: play count by level
    # target: number of sessions before upgrading
    # target: number of plays before upgrading
    # target: length of time before upgrading

    chart = alt.Chart(user_levels, title='User Level Comparison')
    chart = chart.mark_boxplot()
    chart = chart.encode(
        x=alt.X(alt.repeat("column"), type='quantitative'),
        y=alt.Y('level', title='Level')
    ).properties(
        width=225,
        height=50
    ).repeat(
        column=['play_count', 'level_sessions']
    )

    return chart

def user_stats(user_levels):

    # get stats for each user by subscription level
    source = user_levels.sort_values(by=['user_id','user_level_index'])
    user = source.groupby(['user_id'])
    agg = pd.DataFrame.from_dict(
        {
            # user count
            'Users': '{}'.format(len(user)),
            # users with only one session
            'Bounce Rate': '{0:.1f}%'.format(
                sum(
                    (user.size()==1) & 
                    (user.nth(0)['level']=='free') & 
                    (user.nth(0)['level_sessions']==1)
                )/user.ngroups*100
            ),
            # users that upgraded from free to paid
            'Conversion Rate': '{0:.1f}%'.format(
                sum(
                    (user.nth(0)['level']=='paid') & (user.nth(1)['level']=='free')
                )/user.ngroups*100
            ),
            # users that return for multiple visits
            'Return Rate': '{0:.1f}%'.format(
                sum(user['level_sessions'].sum()>1)/user.ngroups*100
            )
        }, orient='index', columns=['Value']
    )
    agg['X'] = range(0,len(agg))
    agg['Y'] = 0
    agg.index.name = 'Stat'
    agg = agg.reset_index()

    # single stat panel
    chart = alt.Chart(agg, title='User Engagement')
    value = chart.encode(
        alt.X('X', scale=alt.Scale(domain=[-0.5, len(agg)]), axis=None),
        alt.Y('Y', scale=alt.Scale(domain=[-0.5, 0.5]), axis=None),
    )
    value = value.mark_text(baseline='bottom', size=20).encode(text='Value')
    label = value.mark_text(baseline='top').encode(text='Stat')

    return (value + label).properties(width=400, height=50)

# read user_engagement query
# TODO: move to ETL process to prevent complicated end user query
with open('user_levels.pgsql') as fh:
    user_levels = fh.read()
user_levels = pd.read_sql(user_levels, conn)

# position each chart into final dashboard
dashboard = (
    (
        (dashboard_title() | user_stats(user_levels)) &
        (user_level() | play_level()) &
        user_comparison(user_levels) & 
        (play_hour() | play_trend()).resolve_scale(color='independent')
    ) |
    (
        (user_top25pct() | user_agent()) &
        play_location()
    ).resolve_scale(color='independent')
)

dashboard = dashboard.configure_view(strokeOpacity=0)

# open dashboard in webbrowser
url = os.path.join(os.getcwd(),'dashboard.html')
dashboard.save(url)
webbrowser.open(os.path.join('file:' + url))