import pandas as pd
import requests
import mysql.connector
from mysql.connector import Error
import datetime as dt

#Date and time to use as primary key
date = dt.datetime.now()
stamp = date.strftime('%Y-%m-%d %H:%M:%S')
count = 1
#API key from OpenWeather account
API_key = '{key}'

#City names, co-ordinates and populations, plus column names for the big loop
dic = {'London':[51.509865,-0.118092],'Leeds':[53.79648,-1.54785],
       'Liverpool':[53.41058,-2.97794],'Manchester':[53.48095,-2.23743],'Edinburgh':[55.95206,-3.19648],
       'Birmingham':[52.48142, -1.89983]}
pops = {'London':7556900,'Leeds':455123,'Liverpool':864122,'Manchester':395515,
        'Edinburgh':464990,'Birmingham':984333}
cities = ['London','Leeds','Liverpool','Manchester','Edinburgh','Birmingham',]

columns = ['Timestamp','City','Population','Country','Latitude','Longitude']
columns2 = ['Temperature','Temp_min','Temp_max','Pressure','Humidity','Visibility',
		   'Wind_Speed','Wind_degree','Rain(1h)','Rain(3h)','Snow(1h)','Snow(3h)',
            'Weather_ID','Weather_group','Description','Cloudiness','Sunrise','Sunset']
columns3 = ['Air_quality_index','CO','NO','NO2','O3','SO2','NH3','PM2_5','PM10']

def create_db_connection():
    connection = None
    try:
        connection = mysql.connector.connect(
            host='127.0.0.1',
            user='root',
            passwd='{password}',
            database='weather')
    except Error as err:
        print(f"Error: '{err}'")
    return connection

def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()

conn = create_db_connection()
cur = conn.cursor()


for i in cities:
    #Moved columns into the loop to reset them with each entry
    df1 = pd.DataFrame(columns=columns)
    df2 = pd.DataFrame(columns=columns2)
    df3 = pd.DataFrame(columns=columns3)
    df4 = pd.DataFrame()
    #Use latitude and longitude to gather data per city
    lat = dic[i][0]
    lon = dic[i][1]
    #Gather current weather data, pollution data and geolocation data
    url_weather = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_key}'
    url_poll = f'http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={API_key}'
    url_geo = f'http://api.openweathermap.org/geo/1.0/reverse?lat={lat}&lon={lon}&limit=10&appid={API_key}'
    url_forecast = f'api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={API_key}'
    weather_data = requests.get(url_weather).json()
    poll_data = requests.get(url_poll).json()
    geo_data = requests.get(url_geo).json()

    rise = dt.datetime.fromtimestamp(weather_data['sys']['sunrise'])
    set = dt.datetime.fromtimestamp(weather_data['sys']['sunset'])
    rise = rise.strftime('%Y-%m-%d %H:%M:%S')
    set = set.strftime('%Y-%m-%d %H:%M:%S')

    #4 try clauses for rain and snow measurements - done this way because they are optional measures
    #If there is not significant rainfall, there will be no measurement, not even 0, and a straight
    #pull would fail and break the script
    try:
        rain1 = url_weather['rain']['1h']
    except Exception:
        rain1 = 0
    try:
        rain3 = url_weather['rain']['3h']
    except Exception:
        rain3 = 0
    try:
        snow1 = url_weather['snow']['1h']
    except Exception:
        snow1 = 0
    try:
        rain1 = url_weather['snow']['3h']
    except Exception:
        snow3 = 0

    #Gather data into one dataframe per API
    df1.loc[len(df1)] = stamp,\
                      geo_data[0]['name'].replace('City of ',''),\
                      pops[i],\
					  geo_data[0]['country'],\
                      lat,\
                      lon
    df2.loc[len(df2)] = (weather_data['main']['temp']- 273.15), \
                        (weather_data['main']['temp_min']- 273.15), \
                        (weather_data['main']['temp_max']- 273.15),\
						weather_data['main']['pressure'],\
						weather_data['main']['humidity'], \
						weather_data['visibility'], \
						weather_data['wind']['speed'], \
						weather_data['wind']['deg'], \
                        rain1, \
                        rain3, \
                        snow1,\
                        snow3,\
						weather_data['weather'][0]['id'], \
						weather_data['weather'][0]['main'], \
						weather_data['weather'][0]['description'], \
						weather_data['clouds']['all'],\
                        rise,\
                        set

    df3.loc[len(df3)] = poll_data['list'][0]['main']['aqi'],\
                        poll_data['list'][0]['components']['co'], \
                        poll_data['list'][0]['components']['no'], \
                        poll_data['list'][0]['components']['no2'], \
                        poll_data['list'][0]['components']['o3'], \
                        poll_data['list'][0]['components']['so2'], \
                        poll_data['list'][0]['components']['nh3'], \
                        poll_data['list'][0]['components']['pm2_5'], \
                        poll_data['list'][0]['components']['pm10']
    #All dataframes combined
    df4 = pd.concat([df1, df2, df3], axis=1).set_index('Timestamp')

    #BIG lists of columns for SQL tables
    drop_table = f"""DROP TABLE IF EXISTS {i}"""
    create_table = f"""CREATE TABLE IF NOT EXISTS {i} (Timestamp Datetime PRIMARY KEY,
    City VARCHAR(255), Population INT, Country VARCHAR(10),Latitude FLOAT,
    Longitude FLOAT,Temperature FLOAT,Temp_min FLOAT,Temp_max FLOAT,Pressure INT, Humidity INT,
    Visibility INT, Wind_speed FLOAT,Wind_degree FLOAT, Rain_1h FLOAT,Rain_3h FLOAT,
    Snow_1h FLOAT,Snow_3h FLOAT, Weather_ID INT, Weather_group VARCHAR(255),
    Description VARCHAR(255), Cloudiness INT, Sunrise Datetime, Sunset Datetime,
    Air_quality_index INT, CO FLOAT, NO FLOAT, NO2 FLOAT,
    O3 FLOAT, SO2 FLOAT, NH3 FLOAT, PM2_5 FLOAT, PM10 FLOAT);"""

    #Create connection and upload data

    #execute_query(conn,drop_table)# - Comment this part for uploads to stay
    execute_query(conn,create_table)
    #Using this method as making a query for execute_query wasn't working
    cur.execute(f"""INSERT INTO {i} (Timestamp,City,Population,Country,Latitude,Longitude,Temperature,
    Temp_min,Temp_max,Pressure,Humidity,Visibility,Wind_Speed,Wind_degree, Rain_1h,Rain_3h,Snow_1h,Snow_3h,
    Weather_ID,Weather_group,Description,Cloudiness,Sunrise,Sunset,Air_quality_index,CO,NO,NO2,O3,SO2,NH3,PM2_5,PM10) 
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
    (stamp, str(df4['City'].values[0]),int(df4['Population'].values[0]),
    str(df4['Country'].values[0]),float(df4['Latitude'].values[0]),float(df4['Longitude'].values[0]),
    float(df4['Temperature'].values[0]),float(df4['Temp_min'].values[0]),float(df4['Temp_max'].values[0]),
    int(df4['Pressure'].values[0]),float(df4['Humidity'].values[0]),int(df4['Visibility'].values[0]),
    float(df4['Wind_Speed'].values[0]),int(df4['Wind_degree'].values[0]), float(df4['Rain(1h)'].values[0]),
    float(df4['Rain(3h)'].values[0]),float(df4['Snow(1h)'].values[0]),float(df4['Snow(3h)'].values[0]),
    str(df4['Weather_ID'].values[0]),
    str(df4['Weather_group'].values[0]),str(df4['Description'].values[0]),str(df4['Cloudiness'].values[0]),
    rise,set,
    str(df4['Air_quality_index'].values[0]),str(df4['CO'].values[0]),str(df4['NO'].values[0]),str(df4['NO2'].values[0]),
    str(df4['O3'].values[0]),str(df4['SO2'].values[0]),str(df4['NH3'].values[0]),str(df4['PM2_5'].values[0]),
    str(df4['PM10'].values[0])))

    #save changes
    conn.commit()

conn.close()
