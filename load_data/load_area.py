import csv
import mysql.connector


db = mysql.connector.connect(
    user='root',
    passwd='dsci551',
    host='127.0.0.1',
    database='project',
    auth_plugin='mysql_native_password'
)


with open('data/area.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter = ',')
    next(csv_reader)
    all_value = []
    for row in csv_reader:
        print(row)
        value = (row[1], row[2], row[3], row[4], row[5], row[6])
        all_value.append(value)

query = 'insert into area(area_id, area_name, lat_min, lat_max, lon_min, lon_max) values(%s, %s, %s, %s, %s, %s)'

mycursor = db.cursor()
mycursor.executemany(query, all_value)
db.commit()
