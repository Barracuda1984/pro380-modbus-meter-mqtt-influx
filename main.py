#!/usr/bin/env python3
import time 
import struct
import json
#import paho.mqtt.client

import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

#from serial import Serial, PARITY_EVEN

#from umodbus.client.serial import rtu
import socket

from umodbus import conf
from umodbus.client import tcp


def get_data(start = 0x4000, length = 0x02):
    message = tcp.read_holding_registers(slave_id = 1, starting_address = start, quantity = length)
    response = tcp.send_message(message, sock)

    count = 0
    data = {}

    while count < len(response):
        h = format(response[count + 0], '04X')
        l = format(response[count + 1], '04X')
        d = h + l
        val = struct.unpack('!f', bytes.fromhex(d))[0]
        key = str(format(start + count, '04X'))
        data[key] = val
        count += 2

    return data

def get_tariff():
    message = tcp.read_holding_registers(slave_id = 1, starting_address = 0x6048, quantity = 1)
    response = tcp.send_message(message, sock)
    tariff = [-1, 1, 0]
    print(response)
    return {"tariff_val" : tariff[response[0]]}

# MODBUS serial RTU
#serial_port = Serial(port='/dev/ttyUSB485', baudrate=9600, parity=PARITY_EVEN, stopbits=1, bytesize=8, timeout=1)

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(('192.168.3.75', 502))
print("Serial port connected")

# MQTT
#mqtt_host_local = "192.168.1.112"
#mqttc = paho.mqtt.client.Client()
#mqttc.connect(mqtt_host_local, 1883, 60)
#mqttc.loop_start()

#print("MQTT Connected")

# InfluxDB
token = "UXocfbEOCSAEuKUUIHOhFmLfzHG7y29Y5Mj2FTe9VpAQ87ngsr5SHSllBqpiEVsix3c_NIQd10Zhz5wwdV59Gg=="
org = "electricity"
url = "http://192.168.1.200:8086"
influx = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
#influx = InfluxDBClient(host='192.168.1.200', port=8086, token=token, org=org)

print("InfluxDBclinet")
#print(influx.get_list_database())
#influx.switch_database('electricity')




print("Influx connected")

once = True
counter = 0
bucket="heatingelconsuption"


while once:
    power_values = get_data(start = 0x5000, length = 0x32)
    energy_values = get_data(start = 0x6000, length = 0x3c)
    tariff = get_tariff()

#    mqttc.publish("electricity/power", json.dumps(power_values))
#    mqttc.publish("electricity/energy", json.dumps(energy_values))
#    mqttc.publish("electricity/tariff", json.dumps(tariff))

    if counter % 30 == 0:
        metrics = {}
        metrics["measurement"] = "power"
        metrics["tags"] = {"tag": "test"}
        print(power_values)

        #metrics["fields"] = {**power_values, **energy_values, **tariff}

        # Append only specific values
        metrics["fields"] = {'5000': energy_values['5000'],
                             '5002': energy_values['5002'],
                             '5004': energy_values['5004'],
                             '5006': energy_values['5006'],
                            '5012': power_values['5012'],
                            '5014': power_values['5014'],
                            '5016': power_values['5016'],
                            '5018': power_values['5018'],

                            '6000': energy_values['6000'],
                            '6002': energy_values['6002'],
                            '6004': energy_values['6004'],

                            '6018': energy_values['6018'],

                            'tariff_val': tariff['tariff_val']
                            }

       
        write_api = influx.write_api(write_options=SYNCHRONOUS)
        write_api.write(bucket=bucket, org="electricity", record=[metrics])
        #influx.write_points([metrics])
        print("influx")

    #once = False
    print("counter", counter, " modulo", counter % 30)
    counter += 1
    time.sleep(1)

#serial_port.close()
sock.close()
