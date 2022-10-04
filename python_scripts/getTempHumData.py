import sqlite3

import paho.mqtt.client as mqtt
from prometheus_client import start_http_server, Summary, Gauge
import time
import datetime
import sqlite3
import os
# from dotenv import load_dotenv
from sqlite3 import Error


# load_dotenv()

# Prometheus Variables
temp = Gauge('outdoor temperature', 'Weather Station Temperature')
hum = Gauge('outdoor humidity', 'Weather Station Humidity')


# MQTT Config Variables
MQTT_ADDRESS = '192.168.0.101'  # os.environ.get("MQTT_ADDRESS")
MQTT_USER = 'spolancom'  # os.environ.get("MQTT_USER")
MQTT_PASSWORD = 'm15cuy1t0s'  # os.environ.get("MQTT_PASSWORD")
MQTT_TOPIC = '/mkr1010/values/temperature'
MQTT_REGEX = 'home/([^/]+)/([^/]+)'
MQTT_CLIENT_ID = 'Zeus'


def on_connect(client, userdata, flags, rc):
    """
    Run the following when a client connects
    :param client:
    :param userdata:
    :param flags:
    :param rc:
    :return:
    """
    print('Connected with result code:' + str(rc))
    # Subscribe mqtt to the following variables
    client.subscribe([('/mkr1010/values/temperature', 1), ('/mkr1010/values/humidity', 1)])


def process_request(msg):
    """
    A function to read the published data over mqtt
    :param msg:
    :return:
    """
    timeVal = datetime.datetime.now()
    # print the timestamp to make sure it is working
    print("Current time: ", timeVal)
    msgStr = str(msg.payload)
    goodMsg = msgStr[2:-1]
    print(msg.topic + ' ' + goodMsg)

    # Make sure we assosite prometheus logs with the correct mqtt variable
    # this publishes the mqtt variables to a prometheus gauge
    # also inser the data into the SQLite table
    if msg.topic == '/mkr1010/values/temperature':
        temp.set(msg.payload)
        sqlMsg = (str(timeVal), str(goodMsg), None, None, None)
        insert_database(sqlMsg)
    elif msg.topic == '/mkr1010/values/humidity':
        hum.set(msg.payload)
        sqlMsg = (str(timeVal), None, str(goodMsg), None, None)
        insert_database(sqlMsg)
    else:
        print('Incorrect topic')


def on_message(client, userdata, msg):
    """
    Run the following command when a MQTT message is received
    :param client:
    :param userdata:
    :param msg:
    :return:
    """
    process_request(msg)


def setup_database():
    """ Setup the database for storing the data sent by mqtt"""
    databasePath = '/tmp/mqtt.sqlite'  # os.environ.get("SQL_PATH")
    # databasePath = str(databasePath) + "/mqtt.sqlite"

    conn = None
    try:
        conn = sqlite3.connect(databasePath)
    except Error as e:
        print(e)

    return conn


def create_table(database_path):
    """ Make the SQLite table if doesn't exist"""
    sql_create_table = "CREATE TABLE IF NOT EXISTS weatherData (id integer PRIMARY KEY, timedat text NOT NULL, temperature text, humidity text);"
    conn = setup_database()

    if conn is not None:
        c = conn.cursor()
        c.execute(sql_create_table)
    else:
        print('Error! Cannot create the database connection.')


def insert_database(sqlMsg):
    """ Save the weather data into a database """
    databasePath = '/tmp/mqtt.sqlite'
    # databasePath = str(databasePath) + "/mqtt.sqlite"

    conn = sqlite3.connect(databasePath)

    sql = 'INSERT INTO weatherData(timedat, temperature, humidity) VALUES (?, ?, ?)'

    cur = conn.cursor()
    cur.execute(sql, sqlMsg)
    conn.commit()


def main():
    # Start Prometheus server
    start_http_server(8000)
    # setup the SQLite database
    databasePath = setup_database()
    create_table(databasePath)

    # start mqtt client
    mqtt_client = mqtt.Client(MQTT_CLIENT_ID)
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)

    # Specify what programs to call when mqtt conditions are met
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    # setup mqtt on a port
    mqtt_client.connect(MQTT_ADDRESS, 1883)
    # keep running forever
    mqtt_client.loop_forever()


if __name__ == '__main__':
    main()
