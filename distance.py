import numpy as np
import pandas as pd
import os
import re
from OSMPythonTools.overpass import Overpass
import geopandas as gpd
from shapely.geometry import Polygon, LineString, Point
from timeit import default_timer as timer

# SF Bay
West = '-122.6126'
North = '38.2182'
East = '-121.7378'
South = '37.2066'

# # Puget Sound
# West = '-122.755863'
# North = '48.1162201'
# East = '-121.7631845'
# South = '46.8555182'

overpass = Overpass()

# filename = 'map/station_list_test.csv'
# filename = 'map/station_list_2023.csv'
filename = 'map/station_list_2023_SF.csv'

print('Loading in station data')

df = pd.read_csv(filename, dtype={'station_index': str})

df_station_locations = df[['station_index', 'latitude', 'longitude']].drop_duplicates()
df_station_locations['trunc_lat'] = df_station_locations['latitude'].round(1)
df_station_locations['trunc_lon'] = df_station_locations['longitude'].round(1)

trunc_list = list(zip(df_station_locations['trunc_lat'], df_station_locations['trunc_lon']))

trunc_list = list(dict.fromkeys(trunc_list)) # de-duplicate the truncated lat/lon pairs

temp_df = pd.DataFrame(columns=['station_index','way','distance'])

for i in trunc_list:
    center_lat = i[0]
    center_lon = i[1]

    station_filter = (df_station_locations['trunc_lat'] == center_lat) & (
                df_station_locations['trunc_lon'] == center_lon)

    df_station_locations[station_filter].groupby(['trunc_lat', 'trunc_lon']).agg({'station_index': pd.Series.nunique})
    West = center_lon - 0.1
    North = center_lat + 0.1
    East = center_lon + 0.1
    South = center_lat - 0.1

    print('Bounding Box: ', West, North, East, South)

    # Motorways within the Purple Air bounding box
    print('Querying OpenStreetMap')
    OPquery = f'way["highway"="motorway"]({South}, {West}, {North}, {East}); out geom;'
    result = overpass.query(OPquery)
    OPqueryT = f'way["highway"="trunk"]({South}, {West}, {North}, {East}); out geom;'
    result_t = overpass.query(OPqueryT)

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

    print('Filtering for Interstates')

    # Puget Sound Filter for specific freeways
    # for id, item in motorways.items():
    #     ref = item['tags'].get('ref', '')
    #     if (not re.search('I\s*-?\s*5', ref) is None):
    #         interstates[id] = item
    #
    #     if (not re.search('I\s*-?\s*90', ref) is None):
    #         interstates[id] = item
    #
    #     if (not re.search('520', ref) is None):
    #         interstates[id] = item
    #         # print(id, item['tags']['ref'], item['tags'].get('maxspeed'), item['geometry'])

    # all interstates & expressways (aka motorways + trunk roads)
    for id, item in motorways.items():
        ref = item['tags'].get('ref', '')
        interstates[id] = item

    # The simplest way to get all data from a bounding box is to explicitly state so (Link):

    # https://github.com/mocnik-science/osm-python-tools/blob/master/docs/element.md

    station_distance_df = dict()

    print('Calculating distances')

    for i, row in df_station_locations[station_filter].iterrows():
        start = timer()

        station_index = row['station_index']

        # print('station: ' + station_index)

        station_distance_df[station_index] = list()

        point = Point(row['longitude'], row['latitude'])
        stations_gdf = gpd.GeoDataFrame({'geometry': [point,]}, crs='EPSG:4326')
        # stations_gdf = stations_gdf.to_crs('EPSG:6596') # Northern WA
        stations_gdf = stations_gdf.to_crs('EPSG:7131') # SF Bay

        for way, item in interstates.items():
            #print(way, item['geometry']['coordinates'])
            try:
                segments_df = gpd.GeoSeries(

                    [
                        LineString(item['geometry']['coordinates'])
                    ],
                    crs='EPSG:4326'
                )

                # segments_df = segments_df.to_crs('EPSG:6596') # Northern WA
                segments_df = segments_df.to_crs('EPSG:7131')  # SF Bay
                dist = segments_df.distance(stations_gdf)[0]/1000.0

                station_distance_df[station_index].append(
                    {
                        'way': way,
                        'distance': dist
                    }

                )
            except:
                pass
        end = timer()
        print('station: ' + station_index + ' | calc time: ' + str(end - start) + 's')
    def f(x):
        return(x['distance'])

    # sorted(station_distance_df['3225'], key=f)[0] # example

    nearest = dict()

    print('Filtering for nearest way')

    try:
        for i, item in station_distance_df.items():
            # print(i, item)
            y = sorted(station_distance_df[i], key=f)[0]
            nearest[i] = y
    except:
        for i, item in station_distance_df.items():
            nearest[i] = np.nan
    for i, item in nearest.items():
        print('Nearest way to station', i +':',item['way'],'| Distance:', str(item['distance'])+'km')

    nearest_df = pd.DataFrame.from_dict(nearest, orient='index').reset_index()
    nearest_df = nearest_df.rename(columns={'index':'station_index'})

    temp_df = pd.concat([temp_df,nearest_df]).drop_duplicates()

# de-dupe for stations that show up in 2 bounding boxes, to take the lower of the two distances.
temp_df = temp_df.groupby('station_index').min('distance')

df = df.merge(temp_df, how='left', on='station_index')
try:
    df = df.loc[: ,~df.columns.str.contains('Unnamed', case=False)]
except:
    pass

df['freeway_adjacent_0_5'] = df['distance'] < .5
df['freeway_adjacent_1_0'] = df['distance'] < 1
df['freeway_adjacent_1_5'] = df['distance'] < 1.5
df['freeway_adjacent_2_0'] = df['distance'] < 2
df['freeway_adjacent_2_5'] = df['distance'] < 2.5
df['freeway_adjacent_3_0'] = df['distance'] < 3
df['freeway_adjacent_3_5'] = df['distance'] < 3.5
df['freeway_adjacent_4_0'] = df['distance'] < 4

outfile = 'map/station_distance_2023-SF.csv'
# outfile = 'map/station_distance-2023.csv'

print('writing ' + outfile)

df.to_csv(outfile)
# print('Plotting AQI')
#
#
# plt.figure(figsize=(16,12))
# sns.scatterplot(data=df[df['date'] == '2020-10-31'], x='distance', y='pm2_5_AVG')
#
# plt.savefig('distance.png')