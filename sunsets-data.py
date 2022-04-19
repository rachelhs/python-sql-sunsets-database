# import CSV file to SQL server

from dotenv import load_dotenv
import requests
import os
from mysql.connector import Error
import mysql.connector
import pandas as pd

# import csv file
data = pd.read_csv(r'./data/webcams.csv')
df = pd.DataFrame(data)

# connect to mysql server
def create_server_connection(host_name, user_name, user_password, database):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=database
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

load_dotenv()
pw = os.environ.get('PW')
connection = create_server_connection("localhost", "root", pw, "sunsets")

cursor = connection.cursor(prepared=True)

# create a table for the webcam live data
# define the columns
cursor.execute('''
CREATE TABLE IF NOT EXISTS sunsetsLive (
    SunsetID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    Name VARCHAR(255),
    URL VARCHAR(255),
    live BOOL
)
'''
               )

# insert data from DataFrame into table
for row in df.itertuples():
    cursor.execute('''
        INSERT INTO sunsetsLive (Name, URL)
        VALUES (?, ?)
        ''',
                   (row.Name,
                    row.URL)
                   )

connection.commit()

# check which webcams are live

api_key = os.environ.get('API_KEY')

# for each url, if it is a youtube embed, extract the id and check if the video is live
cursor.execute("SELECT SunsetID, URL FROM sunsetsLive WHERE URL LIKE '%youtube.com/embed%'")
names = cursor.fetchall()
for x in names:
    # extract video id from url
    vid_id = str(x[1]).split('embed/', 1)[1]
    sunset_id = (str(x[0]),)
    # make api request with video id to get live data
    api_request = requests.get(f'https://youtube.googleapis.com/youtube/v3/videos?id={vid_id}&key={api_key}&part=snippet')
    json = api_request.json()
    # if 'items' has no data, video has been removed
    if len(json['items']) > 0:
        live = json['items'][0]['snippet']['liveBroadcastContent']
    else:
        live = 0
    # insert truth values into 'live' column
    if live == 'live':
        cursor.execute("UPDATE sunsetsLive SET live = 1 WHERE SunsetID=?", sunset_id)
    else:
        cursor.execute("UPDATE sunsetsLive SET live = 0 WHERE SunsetID=?", sunset_id)

connection.commit()

cursor.execute("SELECT * FROM sunsetsLive")
print(cursor.fetchall())
