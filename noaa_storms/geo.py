import os
import webbrowser
import glob
import altair as alt
from vega_datasets import data
import pandas as pd
import numpy as np
from scipy.spatial import ConvexHull
import geojson

# load data
counties = alt.topo_feature(data.us_10m.url, 'counties')
storms = glob.glob('storms/*details*.csv')
columns = [
    'EVENT_ID','EPISODE_ID','EVENT_TYPE',
    'YEAR', 'BEGIN_DATE_TIME',
    'STATE','STATE_FIPS','CZ_TYPE','CZ_FIPS','CZ_NAME',
    'MAGNITUDE','MAGNITUDE_TYPE',
    'BEGIN_LAT','BEGIN_LON','END_LAT','END_LON'
]
storms = pd.concat((pd.read_csv(f,usecols=columns) for f in storms))

# filter storm data
storms = storms[storms['STATE']=='FLORIDA']
storms = storms[storms['EVENT_TYPE'].isin([
    'Thunderstorm Wind', 'Lightning', 'Flood', 'Heavy Rain',
    'Tornado', 'Hail',
    'Strong Wind', 'Funnel Cloud', 'Flash Flood',
    'Tropical Storm', 'Tropical Depression', 'High Wind',
    'Dust Devil', 'Hurricane'
])]

# TODO: plot county region for missing lat/lon
storms = storms[storms['EPISODE_ID']==162055]
# storms = storms[storms['BEGIN_LAT'].notna() & storms['BEGIN_LON'].notna()]
# episode = storms[['EPISODE_ID']].value_counts().reset_index()
# storms = storms[storms['EPISODE_ID']==episode.loc[0]['EPISODE_ID']]

# US counties background
background = alt.Chart(counties).mark_geoshape(
    fill='lightgray',
    stroke='white'
)
background = background.transform_calculate(state_id = "(datum.id / 1000)|0")
background = background.transform_filter((alt.datum.state_id)==12)

# combine beginning and ending storm points
event = pd.concat([
    storms[['BEGIN_LON','BEGIN_LAT']].rename(columns={'BEGIN_LON': 'LON', 'BEGIN_LAT': 'LAT'}),
    storms[['END_LON','END_LAT']].rename(columns={'END_LON': 'LON', 'END_LAT': 'LAT'})
])
event = event.dropna()

# storm points
points = alt.Chart(event)
points = points.encode(
    latitude='LAT',
    longitude='LON'
)
points = points.mark_circle()

# outline of storm region
if len(event.values)==2:
    # singular event with only one beginning and end
    boundary = alt.Chart().mark_geoshape()
else:
    hull = ConvexHull(event.values)
    hull = event.values[hull.vertices]
    hull = np.concatenate((hull, np.array([hull[0]])))
    polygon = geojson.Feature(
        geometry=geojson.Polygon([
            hull.tolist()
            # [[-84, 30], [-83, 31], [-84, 31], [-84, 30]]
        ]),
        properties={"name":"abc"}
    )
    geojson = alt.InlineData(
        values=geojson.FeatureCollection([
            polygon, # feature_2
        ]), 
        format=alt.DataFormat(property='features',type='json')
    )
    boundary = alt.Chart(geojson).mark_geoshape(
        filled=False
    ).encode(
        color="properties.name:N"
    )

# combine plots
chart = (background + points + boundary)
chart = chart.project(
    type='mercator',
    scale=8000,
    center = [event['LON'].mean(), event['LAT'].mean()]
    # center=[-84, 26]
)
chart = chart.properties(
        width=600,
        height=600
)

# save dashboard and open in browser
dashboard = chart
# dashboard = dashboard.configure_view(strokeOpacity=0)
fp = os.path.join(os.getcwd(),'dashboard.html')
dashboard.save(fp)
webbrowser.open(os.path.join('file:' + fp))