import paho.mqtt.client as mqtt
import json
import threading
from handlers.meters import Meters
import base64


broker_url = "192.168.4.133"
broker_port = 1883

username = 'admin'
password = '1234'
mqtt.Client.connected_flag = False  # create flag in class
flag_connected = 0

pic_topic = "image/image"
pic_filename = "D:/Dump/motion-snap.jpg"
vid_topic = "video/video"
vid_filename = "C:/Users/Gustavo Serrano/Downloads/videoplayback.mp4"


def subscriptions(client):
    mqtt_topic = [
        ("spBv1.0/DB_Request/DDATA/EDGE/Request", 0),
        ("meter/kw", 0),
        ("meter/kwh", 0),
        ("sites/temp", 0),
        ("meter/#", 0),
        # ("image/image", 0)
        # ("meter/31413", 0),
        # ("meter/31428", 0),
        # ("meter/32011", 0),
        # ("meter/34955", 0),
        # ("meter/34956", 0),
        # ("meter/34972", 0),
        # ("meter/34984", 0),
        # ("meter/34998", 0),
        # ("meter/36006", 0),
        # ("meter/39602", 0),
        # ("meter/39611", 0),
    ]
    client.subscribe(mqtt_topic)


def on_kwh_message(client, userdata, msg):
    # print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
    json_msg = json.loads(msg.payload.decode())
    Meters().insert_kwh(json_msg)


def on_kw_message(client, userdata, msg):
    # print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
    json_msg = json.loads(msg.payload.decode())
    Meters().insert_kw(json_msg)


def on_meter_message(client, userdata, msg):
    print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
    json_msg = json.loads(msg.payload.decode())
    Meters().insert_meter(json_msg)


def on_temp_message(client, userdata, msg):
    print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
    json_msg = json.loads(msg.payload.decode())
    Meters().insert_temp(json_msg)


# def on_meter_date_message(client, userdata, msg):
#     print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
#     s = msg.payload.decode()
#     s = s.replace("\'", "\"")
#     s = s.replace("\"{", "{")
#     s = s.replace("\"}\"}", "\"}}")
#     fixed_json = json.loads(s)
#     values = fixed_json['metrics'][0]['value']
#     kwh_data = Meters().retrieve_meterkwh_by_date(values)
#     client.publish("history/kwh", str(kwh_data))


def on_meter_request_message(client, userdata, msg):
    # print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
    s = msg.payload.decode()
    # print(s)
    s = s.replace("\'", "\"")
    # print(s)
    # s = s.replace("\"{", "{")
    # print(s)
    # s = s.replace("\"}\"}", "\"}}")
    # print(s)
    fixed_json = json.loads(s)
    values = fixed_json['metrics'][0]['value']
    kwh_data = Meters().retrieve_meter_info(str(values))
    print(kwh_data)
    client.publish("history/kwh", str(kwh_data))


def on_image_message(client, userdata, message):
    print("Receiving message")
    print(message.topic)
    decoded_data = base64.b64decode((message.payload.decode()))
    img_file = open('image.mp4', 'wb')
    img_file.write(decoded_data)
    img_file.close()


def connect_mqtt():
    def on_disconnect(client, userdata, rc):
        global flag_connected
        flag_connected = False
        print("Client Got Disconnected")

    def on_publish(client, userdata, mid):
        print("JSON published")

    def on_message(client, userdata, message):
        print("Message received: " + message.payload.decode())

    def on_subscribe(client, userdata, mid, granted_qos):
        print("Subscribed to: ")

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            global flag_connected
            flag_connected = True
            subscriptions(client)
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt.Client("to_and_from_db")
    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect
    subscriptions(client)
    client.connect(broker_url, broker_port)
    return client


def run():
    client = connect_mqtt()
    kw_callback = threading.Thread(target=client.message_callback_add("meter/kw", on_kw_message), daemon=True)
    kwh_callback = threading.Thread(target=client.message_callback_add("meter/kwh", on_kwh_message), daemon=True)
    temp_callback = threading.Thread(target=client.message_callback_add, args=("sites/temp", on_temp_message),
                                     daemon=True)
    meter_callback = threading.Thread(target=client.message_callback_add, args=("meter/#", on_meter_message,),
                                      daemon=True)
    request_callback = threading.Thread(target=client.message_callback_add,
                                        args=("spBv1.0/DB_Request/DDATA/EDGE/Request", on_meter_request_message,),
                                        daemon=True)
    video_callback = threading.Thread(target=client.message_callback_add, args=("image/image", on_image_message,),
                                      daemon=True)
    request_callback.start()
    temp_callback.start()
    meter_callback.start()
    kw_callback.start()
    kwh_callback.start()
    #video_callback.start()
    try:
        while True:
            #      kw_callback.start()
            #      kwh_callback.start()
            #      client.message_callback_add("meter/31413", on_kw_message)
            #      client.message_callback_add("meter/31428", on_kwh_message)
            #      client.message_callback_add("meter/#", on_meter_message)
            #      client.message_callback_add("meter/31413", on_meter_message)
            #      client.message_callback_add("meter/31428", on_meter_message)
            #      client.message_callback_add("meter/32011", on_meter_message)
            #      client.message_callback_add("meter/34955", on_meter_message)
            #      client.message_callback_add("meter/34956", on_meter_message)
            #      client.message_callback_add("meter/34972", on_meter_message)
            #      client.message_callback_add("meter/34984", on_meter_message)
            #      client.message_callback_add("meter/34998", on_meter_message)
            #      client.message_callback_add("meter/36006", on_meter_message)
            #      client.message_callback_add("meter/39602", on_meter_message)
            #      client.message_callback_add("meter/39611", on_meter_message)
            client.loop_start()
    except KeyboardInterrupt:
        print("Ending")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    run()
