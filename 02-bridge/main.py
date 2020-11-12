#!/usr/bin/env python3

"""A MQTT to InfluxDB Bridge

This script receives MQTT data and saves those to InfluxDB.

"""

import re
import json
import math
import time
from typing import NamedTuple

import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient

INFLUXDB_ADDRESS = 'influxdb'
INFLUXDB_USER = 'root'
INFLUXDB_PASSWORD = 'root'
INFLUXDB_DATABASE = 'treadmill2'

MQTT_ADDRESS = 'mosquitto'
MQTT_USER = 'mqttuser'
MQTT_PASSWORD = 'mqttpassword'
MQTT_TOPIC = 'accel/data'  # [bme280|mijia]/[temperature|humidity|battery|status]
MQTT_REGEX = 'accel/data'
MQTT_CLIENT_ID = 'MQTTInfluxDBBridge'

influxdb_client = InfluxDBClient(INFLUXDB_ADDRESS, 8086, INFLUXDB_USER, INFLUXDB_PASSWORD, None)


class SensorData(NamedTuple):
    device: str
    accel_x: float
    accel_y: float
    accel_z: float
    total_g: float
    gyro_yaw: int
    gyro_pitch: int
    gyro_roll: int


def on_connect(client, userdata, flags, rc):
    """ The callback for when the client receives a CONNACK response from the server."""
    print('Connected with result code ' + str(rc))
    client.subscribe(MQTT_TOPIC)


def on_message(client, userdata, msg):
    """The callback for when a PUBLISH message is received from the server."""
    #print(msg.topic + ' ' + str(msg.payload))
    sensor_data = _parse_mqtt_message(msg.topic, msg.payload.decode('utf-8'))
    #print("on_message", sensor_data)
    if sensor_data is not None:
        _send_sensor_data_to_influxdb(sensor_data)


def _parse_mqtt_message(topic, payload):
    #match = re.match(MQTT_REGEX, topic)
    payload = json.loads(payload)
    #if match:
    if True:
        #print("constructing SensorData", payload)
        device = payload['device']
        #print(device)
        #payload = payload['notification']
        #params = payload['parameters']
        accel_x = payload['acceleration']['x']
        accel_y = payload['acceleration']['y']
        accel_z = payload['acceleration']['z']
        total_g = math.sqrt(accel_x**2 + accel_y**2 + accel_z**2)
        gyro_yaw = payload['gyro']['yaw']
        gyro_pitch = payload['gyro']['pitch']
        gyro_roll = payload['gyro']['roll']
        #print(device, accel_x, accel_y, accel_z, total_g, gyro_yaw, gyro_pitch, gyro_roll)
        return SensorData(device,accel_x,accel_y,accel_z,total_g,gyro_yaw,gyro_pitch,gyro_roll)
    else:
        return None


def _send_sensor_data_to_influxdb(sensor_data):
    #print(sensor_data.device)
    json_body = [
        {
            'measurement': 'acceleration',
            'tags': {
                'device': sensor_data.device
                },
            'fields': {
                'x': sensor_data.accel_x,
                'y': sensor_data.accel_y,
                'z': sensor_data.accel_z,
                'g': sensor_data.total_g
                }
        },
        {
            'measurement': 'gyro',
            'tags': {
                'device': sensor_data.device
                },
            'fields': {
                'yaw': sensor_data.gyro_yaw,
                'pitch': sensor_data.gyro_pitch,
                'roll': sensor_data.gyro_roll
            }
        }
    ]
    print(json_body)
    influxdb_client.write_points(json_body, protocol='json')


def _init_influxdb_database():
    databases = influxdb_client.get_list_database()
    if len(list(filter(lambda x: x['name'] == INFLUXDB_DATABASE, databases))) == 0:
        influxdb_client.create_database(INFLUXDB_DATABASE)
    influxdb_client.switch_database(INFLUXDB_DATABASE)


def main():
    _init_influxdb_database()
    print('connected to influx')
    mqtt_client = mqtt.Client(MQTT_CLIENT_ID)
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.connect(MQTT_ADDRESS, 1883)
    print('connected to mqtt')
    mqtt_client.loop_forever()


if __name__ == '__main__':
    print('MQTT to InfluxDB bridge')
    #time.sleep(30)
    main()
