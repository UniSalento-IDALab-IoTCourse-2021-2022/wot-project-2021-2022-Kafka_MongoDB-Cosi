import json
import time

import paho.mqtt.client as mqtt
from confluent_kafka import Producer

yourapikey = "GAFQE22EYTOO4I7O"
yoursecret = "lBW8dZ1+fZmNRFa5iQbxmFViLXHdEzS0yZRgV/I1EM8F5jezhsJhbgM52r+eLs/f"

conf = {'bootstrap.servers': "pkc-6ojv2.us-west4.gcp.confluent.cloud:9092",
        'client.id': "bridge", 'security.protocol': 'SASL_SSL', 'sasl.mechanism': 'PLAIN',
        'sasl.username': yourapikey, 'sasl.password': yoursecret}

producer = Producer(conf)


# callback function
def on_message(client, userdata, message):
    # print("Received MQTT message: ", message.payload)
    msg_dict = json.loads(message.payload)
    producer.produce("RSSI", message.payload, partition=msg_dict["id"])
    print("KAFKA: Just published " + str(message.payload) + " to topic RSSI")


# client connection
mqtt_broker = "mqtt.eclipseprojects.io"
mqtt_client = mqtt.Client("Bridge")
mqtt_client.connect(mqtt_broker)

mqtt_client.subscribe("IoT_Project/RSSI")
mqtt_client.on_message = on_message

# loop with timer (if not received message for 5 seconds, close)
mqtt_client.loop_start()
run = True
TIMEOUT = 5  # seconds
while run:
    mqtt_client._msgtime_mutex.acquire()
    last_msg_in = mqtt_client._last_msg_in
    mqtt_client._msgtime_mutex.release()
    now = time.monotonic()
    if now - last_msg_in > TIMEOUT:
        mqtt_client.disconnect()
        mqtt_client.loop_stop()
        run = False
    else:
        time.sleep(1)
