import os
import webbrowser
import geojson
import altair as alt

polygon = geojson.Feature(
    geometry=geojson.Polygon([[
        [0, 0], [1, 0], [1, 1], [0, 1], [0, 0]
    ]]),
    properties={"name":"abc"}
)
geojson = alt.InlineData(
    values=geojson.FeatureCollection([
        polygon, # feature_2
    ]), 
    format=alt.DataFormat(property='features',type='json')
)

boundary = alt.Chart(geojson).mark_geoshape(
).encode(
    color="properties.name:N"
).project(
    type='identity', reflectY=True
)

dashboard = boundary
dashboard = dashboard.configure_view(strokeOpacity=0)

# save then open dashboard in webbrowser
fp = os.path.join(os.getcwd(),'dashboard.html')
dashboard.save(fp)
webbrowser.open(os.path.join('file:' + fp))


# airport positions on background
# points = alt.Chart(airports).transform_aggregate(
#     latitude='mean(latitude)',
#     longitude='mean(longitude)',
#     count='count()',
#     groupby=['state']
# ).mark_circle().encode(
#     longitude='longitude:Q',
#     latitude='latitude:Q',
#     size=alt.Size('count:Q', title='Number of Airports'),
#     color=alt.value('steelblue'),
#     tooltip=['state:N','count:Q']
# ).properties(
#     title='Number of airports in US'
# )