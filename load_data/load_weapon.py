import csv
import mysql.connector


db = mysql.connector.connect(
    user='root',
    passwd='dsci551',
    host='127.0.0.1',
    database='project',
    auth_plugin='mysql_native_password'
)


with open('data/weapon.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter = ',')
    next(csv_reader)

    all_value = []
    for row in csv_reader:
        print(row)
        value = (row[0], row[1])
        all_value.append(value)

query = 'insert into weapon(weapon_id, description) values(%s, %s)'

mycursor = db.cursor()
mycursor.executemany(query, all_value)
db.commit()
