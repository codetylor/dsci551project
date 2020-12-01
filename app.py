from flask import Flask, Response, request, make_response
import requests
import json
import datetime

import mysql.connector


db = mysql.connector.connect(
    user='root',
    passwd='dsci551',
    host='127.0.0.1',
    database='project',
    auth_plugin='mysql_native_password'
)

app = Flask(__name__)


@app.route('/')
def hello_world():
    return app.send_static_file('data.html')


@app.route('/home', methods=['GET'])
def main1():
    hour = int(request.args.get("hour"))
    minute = int(request.args.get("minute"))
    latitude = request.args.get("latitude")
    longitude = request.args.get("longitude")
    geolocation = str(latitude) + ", " + str(longitude)
    #print(str(hour) + " " + str(minute) + " " + geolocation)
    if (hour > 0) and (hour < 6):
        interval = "before dawn"
    elif (hour >= 6) and (hour < 7):
        interval = "early morning"
    elif hour == 7:
        if minute <=30:
            interval = "early morning"
        else:
            interval = "morning rush"
    elif (hour > 7) and (hour < 9):
        interval = "morning rush"
    elif (hour >= 9) and (hour < 12):
        interval = "late morning"
    elif (hour >= 12) and (hour < 16):
        interval = "afternoon"
    elif hour == 16:
        if minute <= 30:
            interval = "afternoon"
        else:
            interval = "evening rush"
    elif (hour > 16) and (hour < 19):
        interval = "evening rush"
    elif (hour >= 19) and (hour <= 24):
        interval = "night"

    res = requests.get('http://api.openweathermap.org/data/2.5/weather?lat=' + latitude + '&lon=' + longitude + '&appid=716c9221b1e5214900ab25d41b4ee057')
    data = json.loads(res.text)
    if data:
        weather_id = data["weather"][0]["id"]
        if (weather_id >= 200) and (weather_id < 300):
            weather = "Rain"
        elif (weather_id >= 300) and (weather_id < 400):
            weather = "Rain"
        elif (weather_id >= 500) and (weather_id < 600):
            weather = "Rain"
        elif (weather_id >= 600) and (weather_id < 700):
            weather = "Rain"
        elif (weather_id >= 700) and (weather_id < 800):
            weather = "Mist"
        elif weather_id == 800:
            weather = "Clear"
        elif weather_id>800:
            weather = "Clouds"
    else:
        weather = "Clear"
    print(interval + " " + weather + " " + geolocation)
    if weather:
        print("complete")
        #func1(weather, geolocation, interval)
        response = Response('complete')
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'GET'
        print(response)
        return response
    else:
        print("error")
        response = make_response('fail')
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'GET'
        return response


@app.route('/search', methods=['GET'])
def main2():
    weather = request.args.get("weather")
    areaID = request.args.get("area")
    interval = request.args.get("interval")
    if weather:
        print("complete")
        # func2(weather, areaID, interval)
        response = make_response('complete')
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'GET'
        return response
    else:
        print("error")
        response = make_response('fail')
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'GET'
        return response


@app.route('/laweather', methods=['GET'])
def main3():
    res = requests.get('http://api.openweathermap.org/data/2.5/weather?id=1705545&appid=716c9221b1e5214900ab25d41b4ee057')
    if res:
        data = json.loads(res.text)
        weather = data["weather"][0]["main"]
        temp = str(format((data["main"]["temp"] - 273.15), '.1f'))
        weather_id = data["weather"][0]["id"]
        if (weather_id >= 200) and (weather_id < 300):
            iconurl = "http://openweathermap.org/img/wn/11d@2x.png"
        elif (weather_id >= 300) and (weather_id < 700):
            iconurl = "http://openweathermap.org/img/wn/10d@2x.png"
        elif (weather_id >= 600) and (weather_id < 700):
            iconurl = "http://openweathermap.org/img/wn/50d@2x.png"
        elif weather_id == 800:
            iconurl = "http://openweathermap.org/img/wn/01d@2x.png"
        elif weather_id > 800:
            iconurl = "http://openweathermap.org/img/wn/02d@2x.png"

        response = make_response(weather+'#'+temp+'#'+iconurl)
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'GET'
        return response
    else:
        data = json.loads(res.text)
        response = make_response("Sunny#19#http://openweathermap.org/img/wn/01d@2x.png")
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'GET'
        return response

@app.route('/mysql', methods=['GET'])
def main4():
    table = request.args.get("table")
    query1 = "select COLUMN_NAME from information_schema.COLUMNS where table_name = '"+table+"' and table_schema = 'project'";
    if request.args.get("keyword"):
        keycol = request.args.get("column")
        keyword = request.args.get("keyword")
        query2 = "SELECT * FROM project."+table+" where " + keycol + " like '%" + keyword + "%'"
    else:
        query2 = "SELECT * FROM project." + table
    if request.args.get("order"):
        query2 = query2 + " order by " + request.args.get("order")
    if request.args.get("group"):
        query2 = query2 + " group by " + request.args.get("group")
        query2 = "SELECT *,COUNT(*) as CNT " + query2[9:]
    else:
        query2 = query2 + " limit 50"
    print(query2)
    mycursor = db.cursor()
    mycursor.execute(query1)
    col_name = mycursor.fetchall()  # fetchall() 获取所有记录
    col_list = []
    for m in col_name:
        col_list.append(m[0])
    if request.args.get("group"):
        col_list.append("cnt")
    try:
        mycursor.execute(query2)
        myresult = mycursor.fetchall()  # fetchall() 获取所有记录
        format = '%Y-%m-%d'  # 根据此格式来解析datetime.datetime()对象为时间字符串
        if myresult:
            for x in range(len(myresult)):
                myresult[x] = list(myresult[x])
                for y in range(len(myresult[x])):
                    if type(myresult[x][y]) is datetime.date:
                        myresult[x][y] = myresult[x][y].strftime(format)
            myresult.insert(0, col_list)
            for z in myresult:
                print(z)
            data = json.dumps(myresult)
            response = make_response(data)
            response.headers['Access-Control-Allow-Origin'] = '*'
            response.headers['Access-Control-Allow-Methods'] = 'GET'
            return response
        else:
            response = make_response("nodata")
            response.headers['Access-Control-Allow-Origin'] = '*'
            response.headers['Access-Control-Allow-Methods'] = 'GET'
            return response
    except Exception as e:
        print("invalid query")
        response = make_response("nodata")
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'GET'
        return response


@app.route('/col', methods=['GET'])
def main5():
    table = request.args.get("table")
    query1 = "select COLUMN_NAME from information_schema.COLUMNS where table_name = '"+table+"' and table_schema = 'project'";
    mycursor = db.cursor()
    mycursor.execute(query1)
    col_name = mycursor.fetchall()  # fetchall() 获取所有记录
    col_list = []
    for m in col_name:
        col_list.append(m[0])
    if col_list:
        for z in col_list:
            print(z)
        data = json.dumps(col_list)
        response = make_response(data)
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'GET'
        return response
    else:
        response = make_response("error")
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'GET'
        return response


if __name__ == '__main__':
    app.run()
