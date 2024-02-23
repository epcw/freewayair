import requests
import json
import pandas as pd
from datetime import datetime
from time import sleep

# NOTE to calculate AQI from sensor data = https://community.purpleair.com/t/how-to-calculate-the-us-epa-pm2-5-aqi/877

with open('api_key.txt') as f:
    api = f.read()

headers= {'X-API-Key': api}

# get list of sensors within lat/long bounding box (defined by NW/SE corners)
# Format of bounding boxes is {[(NWlon, NWlat), (SElon, SElat)]}
bounding_boxes = [
    # [(-122.287065,47.574967),(-122.245523,47.506096)],  # rainier beach / valley
    # [(-122.247872,47.64417),(-122.192917,47.598775)]   # bellvue
    # [(-122.755863,48.1162201),(-121.7631845,46.8555182)] # all of puget sound
    [(-122.6126, 38.2182), (-121.7378, 37.2066)]  # SF Bay
    ]

missing_hist_df = pd.DataFrame(columns=['', 'station_index', 'time_stamp', 'pm2.5_alt', 'pm2.5_atm', 'pm2.5_AVG', 'pm10.0_atm'])
missing_hist_filename = 'data/pa_hist_data_2023_missing_SF.csv'

for box in bounding_boxes:
    NW = box[0]
    SE = box[1]
    NWlng = NW[0]
    NWlat = NW[1]
    SElng = SE[0]
    SElat = SE[1]
    print('NW = ',NWlng,',',NWlat)
    print('SE = ',SElng,',',SElat)

    # load in list of stations
    station_list = 'data/station_list_2023_SF.csv'
    # station_list = 'data/station_list_2023.csv'
    df = pd.read_csv(station_list, dtype={'sensor_index': str})
    stations = list()
    for station in df['sensor_index'].items():
        stations.append(station[1])

    hist_stations = list()
    hist_list = 'map/station_list_2023_SF.csv'
    # hist_list = 'map/station_list_2023.csv'
    hist_df = pd.read_csv(hist_list, dtype={'station_index': str}).drop_duplicates(['station_index'])
    for station in hist_df['station_index'].items():
        hist_stations.append(station[1])

    missing_stations = list()
    for i in stations:
        if i in hist_stations:
            pass
        else:
            missing_stations.append(i)

    missing_df = df[(df['sensor_index'].isin(missing_stations))]

    missing_df.to_csv('data/station_list_2023-SF-missing.csv')
    # needed to walk the url request year by year
    unix_year = 31556926
    # today = int(time.time()) # kinda redundant now that I'm only pulling to end date

    # loop over sensors
    for index, row in missing_df.iterrows():
        sensor = row['sensor_index']
        created = row['date_created']
        ended = row['last_seen']
        name = row['name']

        # # 90 days of fire hell in 2020
        # start_timestamp = 1597478400
        # end_timestamp = 1605513600

        # 2023 (PDT)
        start_timestamp = 1672560000
        end_timestamp = 1704095999


        # if (start_timestamp <= created <= end_timestamp) or (start_timestamp <= ended <= end_timestamp):
        print('    ____________','\nSensor: ',name,' (',sensor,')')
        print('Active from ', datetime.fromtimestamp(created).date(),' to ',datetime.fromtimestamp(ended).date())

        # Start at the created date, then pull one year at a time until you hit today
        # for x in range(created, ended, unix_year):
        #     start_timestamp = x
        #     if (x + unix_year) < ended:
        #         end_timestamp = x + unix_year
        #     else:
        #         end_timestamp = ended

        # sample - https://api.purpleair.com/v1/sensors/70735/history?start_timestamp=1672560000&end_timestamp=1704095999&average=1440&fields=pm2.5_alt%2C%20pm2.5_atm%2C%20pm10.0_atm
        # construct URL for API pull
        # hist_url = 'https://api.purpleair.com/v1/sensors/' + str(sensor) + '/history?start_timestamp=' + str(start_timestamp) + '&end_timestamp=' + str(end_timestamp) + '&average=1440&fields=pm2.5_alt%2C%20pm2.5_alt_a%2C%20pm2.5_alt_b%2C%20pm2.5_atm%2C%20pm2.5_atm_a%2C%20pm2.5_atm_b%2C%20pm2.5_cf_1%2C%20pm2.5_cf_1_a%2C%20pm2.5_cf_1_b'
        hist_url = 'https://api.purpleair.com/v1/sensors/' + str(sensor) + '/history?start_timestamp=' + str(start_timestamp) + '&end_timestamp=' + str(end_timestamp) + '&average=1440&fields=pm2.5_alt%2C%20pm2.5_atm%2C%20pm10.0_atm' # Modified - you don't really need the individual channels but the avg, and ATM & ALT are better for outside than CF_1

        # request from API
        try:
            hist_response = requests.get(hist_url, headers=headers)
            print('Pull from ',datetime.fromtimestamp(start_timestamp).date(),' to ',datetime.fromtimestamp(end_timestamp).date(),'| Status code: ', hist_response.status_code)
        except:
            try:
                sleep(62)
                hist_response = requests.get(hist_url, headers=headers)
                print('Pull from ', datetime.fromtimestamp(start_timestamp).date(), ' to ',
                      datetime.fromtimestamp(end_timestamp).date(), '| Status code: ', hist_response.status_code)
            except:
                print(str(sensor) + ": API response failed")
                with open('data/failfile_2023_SF.csv','a') as out:
                    out.write('API fail,',str(sensor) + '\n')
        try:
            # read hist_response
            hist_content = json.loads(hist_response.content)
            # print(hist_content) #testing
            # pull out data & field names
            hist_data = hist_content["data"]
            hist_columns = hist_content["fields"]

            if len(hist_data) == 0:
                print(str(sensor) + ": Data package empty")
                with open('data/failfile_2023_SF.csv', 'a') as out:
                    out.write('empty data,' + str(sensor) + '\n')
            else:
                # dump to dataframe
                temp_df = pd.DataFrame(hist_data, columns=hist_columns)

                # add in station_index
                temp_df.insert(0,'station_index',sensor)

                # calculate average
                print('Calculating pm2.5 avg')
                temp_df['pm2.5_AVG'] = (temp_df['pm2.5_alt'] + temp_df['pm2.5_atm']) / 2

                print('Rounding to 1 decimal place')
                temp_df = temp_df.round(decimals=1)

                # append to the bottom of hist_df
                missing_hist_df = pd.concat([missing_hist_df, temp_df])

                print(temp_df)

                # dump to csv (parquet is better but can't append)
                missing_hist_df.to_csv(missing_hist_filename, mode='a', header='False')
                # hist_df.to_parquet(hist_filename, engine='fastparquet', append=True)
        except:
            try:
                # read hist_response
                hist_content = json.loads(hist_response.content)

                # pull out data & field names
                hist_data = hist_content["data"]
                hist_columns = hist_content["fields"]

                if len(hist_data) == 0:
                    print(str(sensor) + ": Data package empty")
                    with open('data/failfile_2023_SF.csv', 'a') as out:
                        out.write('empty data,' + str(sensor) + '\n')
                else:
                    # dump to dataframe
                    temp_df = pd.DataFrame(hist_data, columns=hist_columns)

                    # add in station_index
                    temp_df.insert(0, 'station_index', sensor)

                    # calculate average
                    print('Calculating pm2.5 avg')
                    temp_df['pm2.5_AVG'] = (temp_df['pm2.5_alt'] + temp_df['pm2.5_atm'])/2

                    print('Rounding to 1 decimal place')
                    temp_df = temp_df.round(decimals=1)

                    # append to the bottom of hist_df
                    missing_hist_df = pd.concat([missing_hist_df, temp_df])
                    print(temp_df)

                    # dump to csv(parquet is better but cannot append)
                    missing_hist_df.to_csv(missing_hist_filename, mode='a', header='False')
                    # hist_df.to_parquet(hist_filename, engine='fastparquet', append=True)

            except:
                print(str(sensor) + ": API response failed")
                with open('data/failfile_2023_SF.csv','a') as out:
                    out.write('fail,' + str(sensor) + '\n')

        # API guidelines is hit once every 1-10min, so setting at just over a minute
        sleep(62)
        # else:
        #     pass

# only necessary if not scraping
# print('Loading ' + hist_filename)
# parqf = pf(hist_filename)
# hist_df = parqf.to_pandas()

# remove all the duplicate column heading rows (this works ASSUMING that all temp_dfs are exactly the same shape and pull the same data in the same order.  Be careful changing the loop above.
print('De-duplicating')
missing_hist_df = missing_hist_df.drop_duplicates(keep='first')

cleaned_filename = 'data/pa_hist_data_cleaned_2023_SF_missing.csv'
# cleaned_filename = 'data/pa_hist_data_cleaned_2023_missing.csv'

print('Writing ' + cleaned_filename)
missing_hist_df.to_csv(cleaned_filename)

# # remove all the duplicate column heading rows (this works ASSUMING that all temp_dfs are exactly the same shape and pull the same data in the same order.  Be careful changing the loop above.
# print('Extracting averages')
# missing_avg_df = pd.read_csv(cleaned_filename).drop_duplicates()
#
# # avg_filename = 'data/pa_hist_data_avg_2023_SF_missing.csv'
# missing_avg_filename = 'data/pa_hist_data_avg_2023_missing.csv'
# print('Writing ' + missing_avg_filename)
# missing_avg_df.to_csv(missing_avg_filename)

# append to existing
main_df = pd.read_csv('map/station_list_2023_SF.csv')
combo_df = pd.concat([main_df, missing_hist_df]).drop_duplicates()
combo_df.to_csv('map/station_list_2023_SF.csv')

