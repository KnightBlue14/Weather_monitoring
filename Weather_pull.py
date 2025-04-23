import mysql.connector
from mysql.connector import Error
import pandas as pd


#City names, co-ordinates and populations, plus column names for the big loop
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
    select = f'SELECT * FROM {i}'
    df = pd.read_sql(select, conn)
    df.to_csv(f'Weather_{i}.csv')

conn.close()
