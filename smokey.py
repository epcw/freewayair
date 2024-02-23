import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# load purpleair data
filename = 'map/station_distance_2023-SF.csv'

df = pd.read_csv(filename, dtype={'station_index': str})

# # tau calculation for pm10_0_diff
# x = list()
# y = list()
#
# h = 0.2
# for tau in [h * (i + 1) for i in range(int((8 - 1 * h) / h + 1))]:
#
#     x.append(tau)
#
#     df = pd.read_csv(filename, dtype={'station_index': str})
#     df_temp = df.groupby(['date']).median()[['pm10_0_atm']]
#     df_temp = df_temp.rename(columns={'pm10_0_atm': 'pm10_0_daily_median'})
#     df = df.merge(df_temp, how='left', on='date')
#     df['freeway_adjacent'] = df['distance'] < tau
#     df['pm10_0_diff'] = df['pm10_0_atm'] - df['pm10_0_daily_median']
#
#     ks = scipy.stats.ks_2samp(df[df['freeway_adjacent'] == True]['pm10_0_diff'],
#                               df[df['freeway_adjacent'] == False]['pm10_0_diff'])
#
#     try:
#         y.append(ks.statistic)
#     except:
#         print(type(ks.statistic))
#
#     'profit'

# # raw pm10_0
#
# ax = sns.histplot(data=df[(df['date'] <= '2023-12-31') & (df['date'] >= '2023-01-01')],hue='freeway_adjacent', x='pm10_0_atm', binwidth=.16,cumulative=True, common_norm=False, fill=False, element='step', stat='probability')
# ax.set_xlim(0,30)

# monthly
for i in range(12):
    plt.figure()
    ax = sns.histplot(
        data=df[pd.to_datetime(df['date']).dt.month == i+1],
        hue='freeway_adjacent_1_5',
        x='pm10_0_diff',
        binwidth=.16,
        cumulative=True,
        common_norm=False,
        fill=False,
        element='step',
        stat='probability',
        label=i+1
    ).set(
        title='month ' + str(i+1),
        xlim=(-15,100)
    )
    fname = '2023-'+str(i+1)+'curve.png'
    plt.savefig(fname)

# all of 2023
plt.figure()
ax = sns.histplot(
    data=df,
    hue='freeway_adjacent_1_5',
    x='pm10_0_diff',
    binwidth=.16,
    cumulative=True,
    common_norm=False,
    fill=False,
    element='step',
    stat='probability'
).set(
    title='pm10 in 2023, for sensors +/- 1.5km from an interstate',
    xlim=(-20,20)
)
plt.savefig('2023-curve-SF.png')

#
# file = 'map/station_distance-2023.csv'
#
# df = pd.read_csv(file)
# print('Plotting AQI')
#
# df = df[df['pm10_0_atm'] < 500]
# # df = df[df['distance'] < 5]
# # df = df[df['date'] == ['2020-10-31']]
#
# plt.figure(figsize=(16,12))
# # sns.regplot(data=df[df['date'] == '2023-10-31'], x='distance', y='pm10_0_atm')
# sns.regplot(data=df, x='distance', y='station_median_pm10_0')
# # sns.regplot(data=df, x='distance', y='pm10_0_atm')
# # sns.regplot(data=df[df['date'] == '2020-10-31'], x='distance', y='pm2_5_AVG')
#
#
# plt.savefig('distance-2023-median.png')

# # df = pd.read_csv('data/aqi_data_stacked.csv')
# df = pd.read_csv('map/station_list.csv')
#
# df.drop(df[df['pm2_5_AVG'] >= 500].index, inplace=True) #filter out impossible values
# print('loading dataframe')
# # df['date'] = pd.to_datetime(df['date'])
#
# fig, ax = plt.subplots(figsize=(30,16))
#
# ax.set(xlabel='Time', ylabel='AQI')
# print('drawing graph: AQI')
# df_wide = df.pivot(index="date", columns="station_index", values="pm2_5_AVG")
# df_wide.head()
# sns.lineplot(data=df_wide)
# # sns.lineplot(x='date', y='pm2_5_AVG', hue='altitude',data=df_wide)
# plt.xticks(rotation=90)
# plt.title('Puget Sound Air Quality Index over time')
# sns.move_legend(ax, "upper left", bbox_to_anchor=(1, 1))
#
# plt.savefig('AQI.png')
#
# fig, ax = plt.subplots(figsize=(30,16))
#
# ax.set_yscale('log')
# ax.set(xlabel='Time', ylabel='AQI')
# print('drawing graph: AQI (log scale)')
# sns.lineplot(x='Observation Time', y='value', hue='measure',data=df)
#
# plt.title('Puget Sound Air Quality Index over time (log scale)')
#
# plt.savefig('AQI_log.png')
#
# df150 = df[(df['value'] >= 150)] #select for days where schools will close
#
# fig, ax = plt.subplots(figsize=(30,16))
#
# ax.set(xlabel='Time', ylabel='AQI')
# print('drawing graph: AQI 150+')
# sns.scatterplot(x='Observation Time', y='value', hue='measure',data=df150, s=10)
#
# plt.title('Puget Sound Air Quality Index: days 150+')
#
# plt.savefig('AQI_150.png')
#
# df100 = df[(df['value'] >= 100)] #select for days with unhealthy air
#
# fig, ax = plt.subplots(figsize=(30,16))
#
# ax.set(xlabel='Time', ylabel='AQI')
# print('drawing graph: AQI 100+')
# sns.scatterplot(x='Observation Time', y='value', hue='measure',data=df100, s=100)
#
# plt.title('Puget Sound Air Quality Index: days 100+')
#
# plt.savefig('AQI_100.png')