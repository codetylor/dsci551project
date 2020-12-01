import json
import pandas as pd
from datetime import datetime
from pytz import timezone
import re
import numpy as np
import ast
from collections import Counter

### Crime dataset
def crime_main(originCrime):
    originCrime.columns = [*originCrime.columns[:-1], 'Location_new']
    crime = originCrime.rename(columns={'Time Occurred': 'Time_Occurred', 'Date Occurred': 'Date_Occurred'})
    crime = originCrime[['Date Occurred', 'Time Occurred', 'Area ID', 'Crime Code', 'Weapon Used Code', 'Address',
                         'Cross Street', 'Location_new']]
    crime = crime.rename(columns={'Time Occurred': 'Time_Occurred', 'Date Occurred': 'Date_Occurred'})
    crime['Date_Occurred'] = pd.to_datetime(crime.Date_Occurred)
    result = []
    for i in crime['Time_Occurred']:
        if 0<= i < 600:
            result.append("before dawn")
        elif 600 <= i < 730:
            result.append("early morning")
        elif 730 <= i < 900:
            result.append("morning rush")
        elif 900 <= i < 1200:
            result.append("late morning")
        elif 1200 <= i < 1630:
            result.append("afternoon")
        elif 1630 <= i < 1900:
            result.append("evening rush")
        elif 1900 <= i <= 2359:
            result.append("night")
    crime['Time_interval'] = result
    crime = crime[crime['Location_new'].str.len() > 7]
    lat = []
    lon = []
    k = []
    cnt = 0
    for i in crime['Location_new']:
        res = str(i).split(", ")
        lat.append(res[0][1:])
        lon.append(res[1][0:-1])
    crime['lat'] = lat
    crime['lon'] = lon
    del crime['Location_new']
    crime = crime.sort_values('Date_Occurred')
    crime = crime.reset_index()
    del crime['index']
    del crime['Cross Street']
    crime = crime.replace(np.nan, 100, regex=True)
    ind = list(range(1,len(crime)+1))
    crime['id'] = ind
    crime['lat'] = crime[['lat']].round(4)
    crime['lon'] = crime[['lon']].round(4)
    crime['lat'] = crime['lat'].astype(float)
    crime['lon'] = crime['lon'].astype(float)
    crime['Weapon Used Code'] = crime['Weapon Used Code'].astype(int)
    crime = crime[['id','Date_Occurred','Area ID','Crime Code','Weapon Used Code','Address','Time_interval','lat','lon']]
    crime.to_csv("crime.csv")

def crime_area(originCrime):
    area = originCrime[['Area ID', 'Area Name']]
    area = area.rename(columns={'Area ID': 'area_id', 'Area Name': 'area_name'})
    area = area.sort_values('area_id')
    area = area.drop_duplicates(subset='area_id')
    lat_min = []
    lat_max = []
    lon_min = []
    lon_max = []
    for i in area.area_id.unique():
        s = str(crime[crime['Area ID'] == i][['lat', 'lon']].min())
        result = re.findall(r"[-+]?\d*\.\d+|\d+", s)
        lat_min.append(result[0])
        lon_min.append(result[1])
        t = str(crime[crime['Area ID'] == i][['lat', 'lon']].max())
        res = re.findall(r"[-+]?\d*\.\d+|\d+", t)
        lat_max.append(res[0])
        lon_max.append(res[1])
    area['lat_min'] = lat_min
    area['lat_max'] = lat_max
    area['lon_min'] = lon_min
    area['lon_max'] = lon_max
    area = area[['area_id', 'area_name', 'lat_min', 'lat_max', 'lon_min', 'lon_max']]
    area['lat_min'] = area['lat_min'].astype(float)
    area['lat_max'] = area['lat_max'].astype(float)
    area['lon_min'] = area['lon_min'].astype(float)
    area['lon_max'] = area['lon_max'].astype(float)
    area.to_csv('areaTable.csv')

def crime_code(originCrime):
    crimecode = originCrime[['Crime Code', 'Crime Code Description']]
    ct = crimecode.sort_values("Crime Code")
    ct1 = ct.drop_duplicates(subset='Crime Code').reset_index()
    ct1 = ct1.rename(columns={'Crime Code': 'crime_code', 'Crime Code Description': 'discription'})
    ct = ct1[['crime_code', 'discription']]
    ct.to_csv('crimeTable.csv', encoding="utf-8")

def crime_weapon(originCrime):
    wt = originCrime[['Weapon Used Code', 'Weapon Description']]
    wt1 = wt.rename(columns={'Weapon Used Code': 'weapon_code', 'Weapon Description': 'discription'})
    wt2 = wt1.sort_values('weapon_code')
    wt3 = wt2.drop_duplicates(subset='weapon_code')
    wt3[wt3['weapon_code'].notna()][['weapon_code','discription']]
    wt3.drop(wt3.tail(1).index,inplace=True)
    wt3.loc[1] = [100, "None"]
    wt3['weapon_code'] = wt3['weapon_code'].astype(int)
    wt3 = wt3.sort_values("weapon_code").reset_index()
    wt3 = wt3[["weapon_code", "discription"]]
    wt3.to_csv("weaponTable.csv")

### Weather dataset
def weather(originWeather):
    for k, v in originWeather.iterrows():
        fmt = "%Y-%m-%d %H%M"
        v['dt_iso'] = v['dt_iso'][:-10]
        datetime_i = datetime.strptime(v['dt_iso'], '%Y-%m-%d %H:%M:%S')
        pac = datetime_i.astimezone(timezone('US/Pacific'))
        new_time = pac.strftime(fmt)
        originWeather.at[k, 'dt_iso'] = new_time
    originWeather['Date'] = pd.to_datetime(originWeather['dt_iso']).dt.date
    originWeather['Time'] = pd.to_datetime(originWeather['dt_iso']).dt.time
    for k, v in originWeather.iterrows():
        fmt = "%H%M"
        i = v['Time']
        new_time = i.strftime(fmt)
        originWeather.at[k, 'Time'] = new_time
    originWeather['Time'] = originWeather['Time'].apply(lambda x: '{0:4}'.format(x))
    weather = originWeather
    wether = []
    for i in weather['weather']:
        #res = ast.literal_eval(i)
        wether.append(i[0]['main'])
    weather['condition'] = wether
    res = []
    df1 = weather.groupby(['Date'])['condition'].apply(', '.join).reset_index()
    for i in df1['condition']:
        sp = i.split()
        cnt = Counter(sp)
        most_occur = cnt.most_common(1)
        res.append(most_occur[0][0][0:-1])
    df1['weather'] = res
    df = df1[['Date','weather']]
    df.to_csv("weatherAfter.csv")

if __name__ == "__main__":
    originCrime = pd.read_csv("Crime_Data.csv", encoding="utf-8")
    originWeather = pd.read_json("la_weather.json", encoding="utf-8")
    crime_main(originCrime)
    crime = pd.read_csv("crime.csv", encoding="utf-8")
    crime_area(originCrime)
    crime_code(originCrime)
    crime_weapon(originCrime)
    weather(originWeather)