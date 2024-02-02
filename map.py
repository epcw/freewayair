import folium
import pandas as pd
import geopandas as gpd

from OSMPythonTools.overpass import Overpass
from shapely.geometry import Polygon, LineString, Point

overpass = Overpass()

outfile = 'index.html'

# load purpleair data
df_filename = 'map/station_distance-2023.csv'

df = pd.read_csv(df_filename, dtype={'station_index': str})
df_halloween = df[df['date'] == '2023-10-31']

# Motorways within the Purple Air bounding box
result = overpass.query('way["highway"="motorway"](46.8555182, -122.755863, 48.1162201, -121.7631845); out geom;') # Northern WA
# result = overpass.query('way["highway"="motorway"](37.2066, -122.6126, 38.2182, -121.7378); out geom;') # SF Bay
result_t = overpass.query('way["highway"="trunk"](46.8555182, -122.755863, 48.1162201, -121.7631845); out geom;') # Northern WA
# result_t = overpass.query('way["highway"="trunk"](37.2066, -122.6126, 38.2182, -121.7378); out geom;') # SF Bay

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
m = folium.Map(location=[47.6578519,-122.3437256], zoom_start=11, tiles="USGS.USTopo") # list of all tile options: https://xyzservices.readthedocs.io/en/stable/gallery.html

# draw freeways
for way, data in interstates.items():
    try:
        # print(way, data['geometry']['coordinates'])
        way_coordinates = list()

        for lon, lat in data['geometry']['coordinates']:
            way_coordinates.append((lat, lon))

        name = data['tags'].get('ref')
    #     print(name)

    #     folium.PolyLine(way_coordinates, tooltip=f"I-? {way}").add_to(m)
        folium.PolyLine(way_coordinates, tooltip=name).add_to(m)
    except:
        pass

# create stations df
df_stations = df_halloween[['name','latitude','longitude','pm10_0_atm']].drop_duplicates()
df_stations.reset_index()
df_stations = df_stations[['name','latitude','longitude','pm10_0_atm']]
stations = df_stations.to_dict(orient="records")

# Draw stations
for row in stations:
    print(row['name'],row['latitude'],row['longitude'])
    latitude = row['latitude']
    longitude = row['longitude']
    name = row['name']
    if row['pm10_0_atm']  <= 50:
        AQI = row['pm10_0_atm']
        color = "green"
    elif AQI <= 100:
        AQI = row['pm10_0_atm']
        color = "yellow"
    elif AQI <= 250:
        AQI = 100 + (row['pm10_0_atm'] -100) * 100 / 150
        color = "red"
    elif AQI <= 350:
        AQI = 200 + (row['pm10_0_atm'] - 250)
        color = "purple"
    elif AQI <= 430:
        AQI = 400 + (row['pm10_0_atm'] - 350) * 100 / 80
        color = "#800000"
    else:
        AQI = 400 + (row['pm10_0_atm'] - 430) * 100 / 80
        color = "#800000"
    popup_text = str(name) + '<br><br>AQI: ' + str(AQI)
    popup = folium.Popup(popup_text, min_width=100,max_width=100)
    # icon = folium.Icon(icon="", icon_color=color, color=color)
    folium.CircleMarker(location=[latitude,longitude], popup=popup, color=color, fill_color=color, fill_opacity=0.7).add_to(m)

m.save(outfile)
