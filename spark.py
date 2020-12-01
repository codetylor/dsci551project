from pyspark.sql import SparkSession
from pyspark import SparkContext as sc
import pyspark.sql.functions as fc
from pyspark.sql.window import Window
import pymysql
import pandas
import sys
import json
import requests

def export_csv(gl, ti):
    conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', password='xiaoyuqi123', database='project')
    cursor = conn.cursor()

    geo_location = gl.split(',')
    lat = float(geo_location[0])
    lon = float(geo_location[1])

    query = 'select crime_id, date_occured, area_id, desc_id, time_inteval from crime where time_inteval = "{}" and area_id in (select area_id from area where lat_min <= {} and lat_max >= {} and lon_min <= {} and lon_max >= {})'.format(ti, lat, lat, lon, lon)

    cursor.execute(query)

    results = pandas.read_sql_query(query, conn)
    results.to_csv("data/filter_crime.csv", index=False)
    return


def export_json(weather):
    url = 'https://dsci551-weather.firebaseio.com/weather.json?orderBy="weather"&equalTo="{}"'.format(weather)
    response1 = requests.get(url)
    file = json.loads(response1.text)
    j = []
    for k,v in file.items():
        j.append(v)
    dump_j = json.dumps(j)
    with open("data/weather.json", "w") as outfile:
        outfile.write(dump_j)
    return


### weather, geolocation, time_inteval
def spark_home():
    output_ctype = []

    spark = SparkSession.builder.appName('551project').getOrCreate()
    df_weather = spark.read.json('data/weather.json')

    df_crime_desc = spark.read.csv('data/crimeDesc.csv', header=True)
    df_filtered_crime = spark.read.csv('data/filter_crime.csv', header=True)

    weather = df_weather.withColumnRenamed('Date', 'weather_date')

    crimeDesc = df_crime_desc.select('crime_code', 'discription').withColumnRenamed('crime_code',
                                                                                    'desc_id').withColumnRenamed(
        'discription', 'description')

    selected_date = weather
    crime_date = selected_date.join(df_filtered_crime, selected_date.weather_date == df_filtered_crime.date_occured)
    #crime_date.show()
    crimd_type_rank = crime_date.groupby('desc_id').agg(fc.count('*').alias('count')).orderBy(fc.desc('count')).limit(5)

    crime_type = crimd_type_rank.join(crimeDesc, crimd_type_rank.desc_id == crimeDesc.desc_id).select('description',
                                                                                                      'count')
    crime_type_precent = crime_type.withColumn('total', fc.sum('count').over(Window.partitionBy())).withColumn(
        'criminal_rate', fc.col('count') / fc.col('total'))
    output_crime_type = crime_type_precent.select('description', 'criminal_rate').orderBy(fc.desc('criminal_rate'))

    collected_ctype = output_crime_type.toPandas()
    crime_type_list = list(collected_ctype['description'])
    ctype_rate_list = list(collected_ctype['criminal_rate'])
    for c_type, c_type_rate in zip(crime_type_list, ctype_rate_list):
        c_type_dic = {}
        c_type_dic['crime_type'] = c_type
        c_type_dic['crime_rate'] = c_type_rate
        output_ctype.append(c_type_dic)

    return output_ctype


def spark_search(an, ti):
    spark = SparkSession.builder.appName('551project').getOrCreate()
    df_weather = spark.read.json('data/weather.json')
    df_crime = spark.read.csv('data/crime.csv', header=True)
    df_area = spark.read.csv('data/area.csv', header=True)
    df_crime_desc = spark.read.csv('data/crimeDesc.csv', header=True)

    weather = df_weather.withColumnRenamed('Date', 'weather_date')
    area = df_area.select('area_id', 'area_name', 'lat_min', 'lat_max', 'lon_min', 'lon_max')
    crimeDesc = df_crime_desc.select('crime_code', 'discription').withColumnRenamed('crime_code',
                                                                                    'desc_id').withColumnRenamed(
        'discription', 'description')

    crime = df_crime.select('id', 'Date_Occurred', 'Area ID', 'Crime Code', 'Weapon Used Code',
                            'Time_interval').withColumnRenamed(
        'id', 'crime_id').withColumnRenamed('Date_Occurred', 'date_occured').withColumnRenamed('Area ID',
                                                                                               'area_id').withColumnRenamed(
        'Crime Code',
        'desc_id').withColumnRenamed('Weapon Used Code',
                                     'weapon_id').withColumnRenamed('Address',
                                                                    'address').withColumnRenamed('Time_interval',
                                                                                                 'time_inteval')

    output_carea = []
    output_ctime_inteval = []
    output_filtered_list = []

    ### get date based on weather condition
    selected_date = weather

    ###filter date on crime table
    crime_selected_date = selected_date.join(crime, selected_date.weather_date == crime.date_occured)
    ###filter time inteval
    crime_selected_date_time = crime_selected_date.filter(crime_selected_date.time_inteval == ti)

    ###get area_id from area based on area name input
    selected_area = area.filter(area.area_name == an).select('area_name', 'area_id')
    crime_selected_date_area = crime_selected_date.join(selected_area, crime_selected_date.area_id == selected_area.area_id)

    #crime_date_area_time = selected_area.join(crime_selected_date_time, selected_area.area_id == crime_selected_date_time.area_id)
    total_bytime = crime_selected_date_area.filter(crime_selected_date_area.time_inteval == ti).groupby('area_name').agg(fc.count('*').alias('sub_total'))
    total_all = crime_selected_date_area.groupby('area_name').agg(fc.count('*').alias('total'))

    collected_sub_total = total_bytime.toPandas()
    collected_total = total_all.toPandas()
    c_farea_rate = int(collected_sub_total['sub_total'][0]) / int(collected_total['total'][0])
    c_farea_dic = {}
    c_farea_dic['crime_filter_area'] = collected_sub_total['area_name'][0]
    c_farea_dic['crime_rate'] = c_farea_rate
    output_filtered_list.append(c_farea_dic)


    crime_groupby_time = crime_selected_date_area.groupby('time_inteval').agg(fc.count('*').alias('count'))
    crime_time_precent = crime_groupby_time.withColumn('total', fc.sum('count').over(Window.partitionBy())).withColumn(
        'criminal_rate', fc.col('count') / fc.col('total'))
    output_time_inteval = crime_time_precent.select('time_inteval', 'criminal_rate').orderBy(fc.desc('criminal_rate'))

    collected_ctime_inteval = output_time_inteval.toPandas()
    time_inteval_list = list(collected_ctime_inteval['time_inteval'])
    time_inteval_rate_list = list(collected_ctime_inteval['criminal_rate'])
    for c_time, c_time_rate in zip(time_inteval_list, time_inteval_rate_list):
        c_time_dic = {}
        c_time_dic['crime_time'] = c_time
        c_time_dic['crime_rate'] = c_time_rate
        output_ctime_inteval.append(c_time_dic)

    ###crime rate by different area, based on time inteval & weather
    crime_groupby_area = crime_selected_date_time.groupby('area_id').agg(fc.count('*').alias('count'))
    crime_area_precent = crime_groupby_area.withColumn('total', fc.sum('count').over(Window.partitionBy())).withColumn('criminal_rate', fc.col('count') / fc.col('total'))
    output_crime_area = crime_area_precent.join(area, area.area_id == crime_area_precent.area_id).select('area_name', 'criminal_rate').orderBy(fc.desc('criminal_rate'))

    collected_carea = output_crime_area.toPandas()
    crime_area_list = list(collected_carea['area_name'])
    carea_rate_list = list(collected_carea['criminal_rate'])
    for c_area, c_area_rate in zip(crime_area_list, carea_rate_list):
        c_area_dic = {}
        c_area_dic['crime_area'] = c_area
        c_area_dic['crime_rate'] = c_area_rate
        output_carea.append(c_area_dic)

    return output_carea, output_ctime_inteval, output_filtered_list


def write_json(c_type, c_area, c_time_inteval, c_filter_area, fileName):

    a = json.dumps(c_type)
    b = json.dumps(c_area)
    c = json.dumps(c_time_inteval)
    d = json.dumps(c_filter_area)

    with open(fileName, 'w') as f:
        f.write(a)
        f.write(b)
        f.write(c)
        f.write(d)
        f.close()

    return


def main(weather, area, gl, ti, flag):
    export_json(weather)

    if flag == 1:
        export_csv(gl, ti)
        print('Mysql Export Done!')
        c_type = spark_home()
        #c_type_rate = json.dumps(c_type)
        url1 = 'https://dsci551-temp.firebaseio.com/crime_type.json'
        requests.put(url1, json=c_type)
        print(c_type)
        return c_type

    else:
        c_area, c_time_inteval, c_filter_area = spark_search(area, ti)
        #area = json.dumps(c_area)
        url2 = 'https://dsci551-temp.firebaseio.com/crime_area.json'
        requests.put(url2, json=c_area)

        #time_inteval = json.dumps(c_time_inteval)
        url3 = 'https://dsci551-temp.firebaseio.com/crime_time_inteval.json'
        requests.put(url3, json=c_time_inteval)

        #filter_area = json.dumps(c_filter_area)
        url4 = 'https://dsci551-temp.firebaseio.com/crime_filter_area.json'
        requests.put(url4, json=c_filter_area)

        print(c_area)
        print(c_time_inteval)
        print(c_filter_area)

        return c_area, c_time_inteval, c_filter_area


#main('Clear', 'West LA', '34.3527, -118.7158', 'night', 0)


