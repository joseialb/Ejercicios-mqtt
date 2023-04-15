"""
Ejercicio 3. Calculo de temperaturas
"""

import random
import time
from paho.mqtt.client import Client
import sys

def on_message(client, userdata, msg):
    try:
        print(msg.topic, msg.payload.decode())
        topic = msg.topic
        t = float(msg.payload)
        userdata['global'].append(t)
        if topic in userdata.keys():
            userdata[topic].append(t)
        else:
            userdata[topic] = [t]
    except ValueError:
        pass
    except Exception as e:
        raise e

def main():
    userdata = {'global' : []}
    client = Client(userdata = userdata)
    client.on_message = on_message

    print(f'Conectando al broker {broker}/{main_topic}')
    client.connect(broker)
    client.subscribe(main_topic)
    client.loop_start()

    while True:
        for topic in list(userdata.keys()):
            temps = userdata[topic]
            if topic == 'global':
                if temps != []:
                    print(f"\nLa temperatura media de todos los sensores ha sido de {media(temps)} grados")
                    print(f"La temperatura maxima de todos los sensores ha sido de {max(temps)} grados")
                    print(f"La temperatura minima de todos los sensores ha sido de {min(temps)} grados\n")
                else:
                    print("\nNingun sensor ha enviado ninguna temperatura\n")
            else:
                if temps != []:
                    print(f"La temperatura media del sensor {topic} ha sido de {media(temps)} grados")
                    print(f"La temperatura maxima del sensor {topic} ha sido de {max(temps)} grados")
                    print(f"La temperatura minima del sensor {topic} ha sido de {min(temps)} grados\n")
                else:
                    print(f"El sensor {topic} no ha enviado ninguna temperatura\n")
                    del userdata[topic]
            temps.clear()
        time.sleep(random.random()*4+4) # Espera entre 4 y 8 segundos

def media(l):
    m = 0
    n = 0
    for i in l:
        m += i
        n += 1
    return m/n if n >0 else 0              
            
if __name__ == '__main__':
    broker = "simba.fdi.ucm.es"
    if len(sys.argv) > 1:
        broker = sys.argv[1]
    main_topic = "temperature/#"
    if len(sys.argv) > 2:
        main_topic = sys.argv[2]
    # main_topic = 'clients/mi_tema/mi_subtema' # temperature no estaba produciendo datos
    main()
