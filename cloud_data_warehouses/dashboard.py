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

    # return chart.properties(width=200, height=50)
    source = pd.DataFrame({'X': [0], 'Y': [0], 'Title': ['User Play Dashboard']})
    chart = alt.Chart(source)
    label = chart.encode(
        alt.X('X', scale=alt.Scale(domain=[-0.5, 0.5]), axis=None),
        alt.Y('Y', scale=alt.Scale(domain=[-0.5, 0.5]), axis=None),
    )
    label = label.mark_text(baseline='bottom', size=26, fontWeight='bold').encode(text='Title')

    return (label).properties(width=200, height=50)

def user_stats(user_levels):

    # get stats for each user by subscription level
    source = user_levels.sort_values(by=['User ID','User Level Index'])
    user = source.groupby(['User ID'])
    agg = pd.DataFrame.from_dict(
        {
            # user count
            'Users': '{}'.format(len(user)),
            # users that return for multiple visits
            'Return Rate': '{0:.1f}%'.format(
                sum(user['Level Sessions'].sum()>1)/user.ngroups*100
            ),
            # users with only one session
            'Bounce Rate': '{0:.1f}%'.format(
                sum(
                    (user.size()==1) & 
                    (user.nth(0)['Level Current']=='free') & 
                    (user.nth(0)['Level Sessions']==1)
                )/user.ngroups*100
            ),
            # users that upgraded from free to paid
            'Upgrade Rate': '{0:.1f}%'.format(
                sum((user.nth(0)['Level Current']=='paid') & (user.nth(1)['Level Current']=='free'))/user.ngroups*100
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

    return (chart + text).properties(width=250, height=60)

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

    return (chart + text).properties(width=250, height=60)  

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

    return (chart + rect).properties(width=250, height=100)
  
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
        color=alt.Color('level', legend=alt.Legend(orient='bottom'), title='Level')
    )

    return chart.properties(width=275, height=150)

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
            scale=alt.Scale(scheme='lightmulti'), title='Plays',
            legend=alt.Legend(orient='bottom')
        )
    )

    text = base.mark_text(baseline='middle').encode(text='playcount')

    return (heatmap + text).properties(width=150, height=100)

def user_comparison(user_levels):

    chart = alt.Chart(user_levels, title='User Level Comparison')
    chart = chart.mark_boxplot(size=20, extent=0.5, outliers=False)
    chart = chart.encode(
        x=alt.X(alt.repeat("column"), type='quantitative'),
        y=alt.Y('Level Category', title='Level', axis=None),
        color=alt.Color('Level Category', legend=alt.Legend(orient='top'))
    ).properties(
        width=250,
        height=100
    ).repeat(
        column=['Plays', 'Level Sessions']
    )

    return chart

# read user_engagement query
def level_query():

    # TODO: move to ETL process to prevent complicated end user query
    with open('user_levels.pgsql') as fh:
        levels = fh.read()
    levels = pd.read_sql(levels, conn)
    # rename for friendly dashboard names
    levels = levels.rename(columns={
        'user_id': 'User ID',
        'user_level_index': 'User Level Index',
        'level_current': 'Level Current',
        'level_previous': 'Level Previous',
        'level_next': 'Level Next',
        'level_sessions': 'Level Sessions',
        'play_count': 'Plays',
        'level_days': 'Level Days'
    })

    # categorize user levels such as upgrades/downgrades
    levels['Level Category'] = None
    levels.loc[(levels['Level Current']=='free') & (levels['Level Next']=='paid'),'Level Category'] = 'Free then Paid'
    levels.loc[(levels['Level Current']=='paid') & (levels['Level Next']=='free'),'Level Category'] = 'Paid then Free'
    levels.loc[(levels['Level Current']=='free') & (levels['Level Next'].isna()),'Level Category'] = 'Free Only'
    levels.loc[(levels['Level Current']=='paid') & (levels['Level Next'].isna()),'Level Category'] = 'Paid Only'

    return levels

user_levels = level_query()

# position each chart into final dashboard
dashboard = (
    (
        (dashboard_title() | user_stats(user_levels)) &
        (user_level() | play_level()) &
        user_comparison(user_levels) & 
        (play_hour() | play_trend()).resolve_scale(color='independent')
    ).resolve_scale(color='independent') |
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