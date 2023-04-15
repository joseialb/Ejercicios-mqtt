"""
Ejercicio 4. Temperaturas y humedades, subscribes dinámicos
"""

from paho.mqtt.client import Client
import sys


def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        print(topic, msg.payload.decode())
        if topic == topic_temp:
            if float(msg.payload) > k0 and not(userdata['Hum?']):
                client.subscribe(topic_hum)
                userdata['Hum?'] = True
            elif float(msg.payload) < k0 and userdata['Hum?']:
                client.unsubscribe(topic_hum)
                userdata['Hum?'] = False
        elif topic == topic_hum:
            if float(msg.payload) > k1:
                client.unsubscribe(topic_hum)
                userdata['Hum?'] = False
    
    except ValueError:
        pass
    except Exception as e:
        raise e

def main():
    userdata = {'Hum?' : False}
    client = Client(userdata = userdata)
    client.on_message = on_message
    print(f'Conectando al broker {broker}/{topic_temp}')
    client.connect(broker)
    client.subscribe(topic_temp)
    client.loop_forever()

if __name__ == '__main__':
    broker = "simba.fdi.ucm.es"
    if len(sys.argv) > 1:
        broker = sys.argv[1]
    
    topic_temp = "clients/temperature/kitchen/temp1"
    if len(sys.argv) > 2:
        topic_temp = sys.argv[2]
    topic_hum = topic_temp.replace("temperature" , "humidity").replace("temp", "hump")
    
    k0 , k1 = 50, 50
    if len(sys.argv) > 4:
        k0, k1 = sys.argv[3], sys.argv[4]
    
    # topic_temp = 'clients/temperature/mi_subtema' # temperature no estaba produciendo datos
    # topic_hum = topic_temp.replace("temperature" , "humidity").replace("temp", "hump")

    main()


