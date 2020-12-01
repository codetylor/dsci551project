import csv
import mysql.connector


db = mysql.connector.connect(
    user='root',
    passwd='dsci551',
    host='127.0.0.1',
    database='project',
    auth_plugin='mysql_native_password'
)

with open('data/crime.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    next(csv_reader)
    all_value = []
    i = 0
    for row in csv_reader:
        i = i + 1
        if i <= 500000:
            continue
        elif i == 1000000:
            break
        else:
            print(row)
            value = (row[1], row[2], row[3], row[4], row[5], row[6],
                     row[7], row[8], row[9])
            all_value.append(value)

query = 'insert into crime(crime_id, date_occured, ' \
        'area_id, desc_id, weapon_id,' \
        'address, time_inteval, lat, lon)values(%s, %s, %s, %s, %s, %s, %s, %s, %s)'

mycursor = db.cursor()
mycursor.executemany(query, all_value)
db.commit()

