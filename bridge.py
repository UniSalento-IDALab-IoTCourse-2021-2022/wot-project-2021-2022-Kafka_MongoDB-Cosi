import json
import time
import requests
import paho.mqtt.client as mqtt

api_url = "http://localhost:8082/topics/RSSI"
headers = {"Content-Type": "application/vnd.kafka.json.v2+json"}


# callback function
def on_message(client, userdata, message):
    print("Received MQTT message: ", message.payload)
    my_json = json.loads(message.payload)
    value = json.dumps(my_json)
    response = requests.post(api_url, json={"records": [{
        "value": value,
        "partition": my_json["id"]}]}, headers=headers)
    print(response.json())


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
