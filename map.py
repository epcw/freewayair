import folium
import folium.plugins as plugins
import pandas as pd
import geopandas as gpd
from folium.plugins import HeatMap

from OSMPythonTools.overpass import Overpass
from shapely.geometry import Polygon, LineString, Point

overpass = Overpass()

outfile = 'index.html'

# load purpleair data
# df_filename = 'map/station_distance-2023.csv'
df_filename = 'map/station_distance_2023-SF-combined.csv'

df = pd.read_csv(df_filename, dtype={'station_index': str})
# df_halloween = df[df['date'] == '2023-10-31']

# Add CA pm10 limit parameter
def CAsafe(row):
    if row['pm10_0_station_median'] and row['pm10_0_atm'] < 50:
        return True
    else:
        return False

df['pm10_CAsafe'] = df.apply(CAsafe, axis=1)

df_temp = df[(df['pm10_CAsafe'] == False)]
df_temp = df_temp[['date', 'station_index', 'pm10_CAsafe']]
df_safe = df_temp.groupby(['station_index']).count()[['date']].reset_index()
df_safe = df_safe.rename(columns={'date':'pm10_CAunsafe_station_days'})
df = df.merge(df_safe, how='left', on=['station_index'])
df['pm10_CAunsafe_station_days'] = df['pm10_CAunsafe_station_days'].fillna(0)

# Motorways within the Purple Air bounding box
# Die Bundesautobahnen Amerikanen
# result = overpass.query('way["highway"="motorway"](46.8555182, -122.755863, 48.1162201, -121.7631845); out geom;') # Northern WA
result = overpass.query('way["highway"="motorway"](37.2066, -122.6126, 38.2182, -121.7378); out geom;') # SF Bay
# result_t = overpass.query('way["highway"="trunk"](46.8555182, -122.755863, 48.1162201, -121.7631845); out geom;') # Northern WA
result_t = overpass.query('way["highway"="trunk"](37.2066, -122.6126, 38.2182, -121.7378); out geom;') # SF Bay

motorways = dict()

for e in result.elements():
    motorways[e.id()] = {
        'type': e.type(),
        'tags': e.tags(),
        'geometry': e.geometry()
    }

for e in result_t.elements():
    motorways[e.id()] = {
        'type': e.type(),
        'tags': e.tags(),
        'geometry': e.geometry()
    }

interstates = dict()
for id, item in motorways.items():
    ref = item['tags'].get('ref', '')
    interstates[id] = item

# initialize map
m = folium.Map()
# m = folium.Map(location=[47.6578519,-122.3437256], zoom_start=10, tiles="USGS.USTopo") # list of all tile options: https://xyzservices.readthedocs.io/en/stable/gallery.html # Puget Sound
m = folium.Map(location=[37.807639,-122.270834], zoom_start=11, tiles="USGS.USTopo") # list of all tile options: https://xyzservices.readthedocs.io/en/stable/gallery.html # SF Bay

# draw freeways
for way, data in interstates.items():
    try:
        # print(way, data['geometry']['coordinates'])
        way_coordinates = list()

        for lon, lat in data['geometry']['coordinates']:
            way_coordinates.append((lat, lon))

        name = data['tags'].get('ref')
        print('This is the Way: ', name)

    #     folium.PolyLine(way_coordinates, tooltip=f"I-? {way}").add_to(m)
        folium.PolyLine(way_coordinates, tooltip=name).add_to(m)
    except:
        print("no way!")
        pass

# # TIME STEP VERSION
# timelist_tempo = []
# # Min-Max Normalization
# df['weight'] = (df['pm10_0_atm']-df['pm10_0_atm'].min())/(df['pm10_0_atm'].max()-df['pm10_0_atm'].min())
#
# weight_list = []
# # for x in df['date']:
# #     time_step = x
#
# for x in df['date'].sort_values().unique():
#     df_temp = df[df['date'] == x]
#     df_date = df_temp.drop_duplicates(subset=['latitude', 'longitude']).reset_index()
#     df_date = df_date[['latitude', 'longitude', 'weight']]
#     weight_list.append(df_date.values.tolist())
#
#     timelist_tempo.append(x)
#
# folium.plugins.HeatMapWithTime(weight_list, radius=50,index=timelist_tempo,
#                                gradient={0.1: 'blue',0.25:'green',0.59: 'yellow',0.75: 'orange', 0.9: 'red'},auto_play=True, max_opacity=.7, min_speed=1,index_steps=1).add_to(m)

# MARKER VERSION
# create stations df
df_stations = df[['name','latitude','longitude','pm10_CAunsafe_station_days']].drop_duplicates()
df_stations.reset_index()
df_stations = df_stations[['name','latitude','longitude','pm10_CAunsafe_station_days']]
stations = df_stations.to_dict(orient="records")

# Draw stations
for row in stations:
    print(row['name'],row['latitude'],row['longitude'])
    latitude = row['latitude']
    longitude = row['longitude']
    name = row['name']
    AQI = row['pm10_CAunsafe_station_days']
    # magnified color scale for visibility
    if AQI < 1:
        color = "green"
    elif AQI <= 5:
        color = "#c7ff2d"
    elif AQI <= 10:
        color = "yellow"
    elif AQI <= 20:
        color = "orange"
    elif AQI <= 30:
        color = "red"
    elif AQI <= 40:
        color = "#800000"
    elif AQI <= 50:
        color = "purple"
    else:
        color = "#800000"

    # # EPA color scale
    # if row['station_median_pm10_0']  <= 50:
    #     AQI = row['station_median_pm10_0']
    #     color = "green"
    # elif AQI <= 100:
    #     AQI = row['station_median_pm10_0']
    #     color = "yellow"
    # elif AQI <= 250:
    #     AQI = 100 + (row['station_median_pm10_0'] -100) * 100 / 150
    #     color = "red"
    # elif AQI <= 350:
    #     AQI = 200 + (row['station_median_pm10_0'] - 250)
    #     color = "purple"
    # elif AQI <= 430:
    #     AQI = 400 + (row['station_median_pm10_0'] - 350) * 100 / 80
    #     color = "#800000"
    # else:
    #     AQI = 400 + (row['station_median_pm10_0'] - 430) * 100 / 80
    #     color = "#800000"
    popup_text = str(name) + '<br><br># unsafe days in 2023: ' + str(AQI)
    popup = folium.Popup(popup_text, min_width=100,max_width=100)
    # icon = folium.Icon(icon="", icon_color=color, color=color)
    folium.CircleMarker(location=[latitude,longitude], popup=popup, color=color, fill_color=color, fill_opacity=0.7, opacity=0.7).add_to(m)
#
# df_hm = df_stations[['latitude','longitude','pm10_CAunsafe_station_days']]
# HeatMap(df_hm,
#         radius=20,
#         min_opacity=0.4,
#         blur = 18
#                ).add_to(folium.FeatureGroup(name='Heat Map').add_to(m))
# folium.LayerControl().add_to(m)

m.save(outfile)
