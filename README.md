# Center for Equitable Policy in a Changing World
## freewayair

### Project Title
Particulate Air Quality near Freeways

#### Description and instructions
This is a project looking at particulate matter pollution in and around freeways, initially in Puget Sound and the San Francisco Bay Area.  It first scrapes Purple Air Data for a specific time period, pulling pm2.5 and pm10 daily averages for all stations within a defined geographical bounding box. Then, after collating and cleaning that data, it is available to graph or build web maps.

#### Filelist
- _purpleair_fetch.py_:
  - Fetches list of stations from PurpleAir within Puget Sound and pulls historical data, saving as _/data/pa_hist_data_avg.csv_
- _data_prep.py_:
  - Takes raw _/data/pa_hist_data_avg.csv_ (or other named files) and outputs collated and cleaned _/map/station_list.csv_.  
- _smokey.py_:
  - Produces line and scatter plots of Clean Are Agency AQI data, using one of the .csv files in _/map_ as a required input.  
- _distance.py_:
  - Calculates distances for each station in _/map/station_list.csv_ from a freeway or trunk road (expressway) and outputs to _/map/station_distance.csv_.
- _map.py_:
  - Builds a map of freeways and stations and outputs _index.html_, which can be pushed to any webserver as a mobile-ready visualizaiton.
- _api_key_sample.txt_:
  - Required input for _purpleair_fetch.py_ Your PurpleAir API GET key goes here in the first line, just raw text, no wrappers.
- _index.html_:
  - Webpage for viz goes here.

### Data source
AQI data from the [Puget Sound Clean Air Agency](https://pscleanair.gov/154/Air-Quality-Data) and combining it with a historical scrape of public Purple Air stations ([API reference](https://api.purpleair.com/#api-sensors-get-sensor-history)).  OSM mapping data and tools from the ([Open Street Maps Overpass API ([API reference](https://wiki.openstreetmap.org/wiki/Overpass_API)).  Web mapping vizualization built with [Folium](https://github.com/python-visualization/folium).

### Principal researchers
Richard W. Sharp\
Patrick W. Zimmerman

#### Codebase
**Data prep & initial analysis**: Python 3.10\
**Mapping calculations**: OSMPythonTools\
**charts and static plots**: Seaborn\
**Vizualization**: Folium

#### Python Package requirements (as well as all their dependencies)
csv\
datetime\
Folium\
geopandas\
matplotlib\
numpy\
os\
OSMPythonTools\
pandas\
requests\
seaborn