import json
import time
import requests
import paho.mqtt.client as mqtt

api_url_RSSI = "http://localhost:8082/topics/zi76opth-RSSI"
api_url_coordinate = "http://localhost:8082/topics/zi76opth-coordinate"
headers = {"Content-Type": "application/vnd.kafka.json.v2+json"}


# callback function
def on_message(client, userdata, message):
    print("Received MQTT message: ", message.payload)
    my_json = json.loads(message.payload)
    value = json.dumps(my_json)

    if message.topic == "IoT_Project/RSSI":
        response = requests.post(api_url_RSSI, json={"records": [{
            "value": value,
            "partition": my_json["id"]}]}, headers=headers)
    else:
        response = requests.post(api_url_coordinate, json={"records": [{
            "value": value,
            "partition": 0}]}, headers=headers)

    print("Sending message to the broker: ", response.json())


# client connection
mqtt_broker = "test.mosquitto.org"
mqtt_client = mqtt.Client("Bridge")
mqtt_client.connect(mqtt_broker)

mqtt_client.subscribe("IoT_Project/RSSI")
mqtt_client.subscribe("IoT_Project/coordinates")
mqtt_client.on_message = on_message

# loop with timer (if not received message for 5 seconds, close)
mqtt_client.loop_start()
run = True
TIMEOUT = 5  # seconds
while run:
    mqtt_client.msgtime_mutex.acquire()
    last_msg_in = mqtt_client.last_msg_in
    mqtt_client.msgtime_mutex.release()
    now = time.monotonic()
    if now - last_msg_in > TIMEOUT:
        mqtt_client.disconnect()
        mqtt_client.loop_stop()
        run = False
    else:
        time.sleep(1)
