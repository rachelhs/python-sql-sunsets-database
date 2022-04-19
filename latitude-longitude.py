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

cursor.execute('''DROP TABLE sunsetsLatLong''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS sunsetsLatLong (
    SunsetID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    Name VARCHAR(255),
    Latitude DECIMAL(11, 8),
    Longitude DECIMAL(11, 8)
)
'''
               )

for row in df.itertuples():
    cursor.execute('''
    INSERT INTO sunsetsLatLong (Name, Latitude, Longitude)
    VALUES (?, ?, ?)
    ''',
    (row.Name, 
    row.Latitude,
    row.Longitude
)
    )

output = []

cursor.execute('''SELECT DISTINCT a.Latitude, a.Longitude from sunsetsLatLong a, sunsetsLive b WHERE (a.Name = b.Name AND b.live=1)''')

for row in cursor:
    output.append((str(row[0]), str(row[1])))

text_file = open("lat-long-working.txt", "w")
for row in output:
    formatted_row = str(row)
    formatted_row = formatted_row.replace("(", "")
    formatted_row = formatted_row.replace(")", "")
    formatted_row = formatted_row.replace("'", "")
    text_file.write(formatted_row + "\n")
text_file.close()
