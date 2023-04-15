"""
Ejercicio 5. Temporizador
Cliente
"""


from paho.mqtt.client import Client
from multiprocessing import Process
import time
import pickle
import sys


def esperarPublicar(client,t,topic,mensaje):
    time.sleep(float(t))
    client.publish(topic.decode(), mensaje.decode())

def on_message(client, userdata, msg):
    try:
        #Suponemos que los mensajes son una lista con los parametros
        t, topic, mensaje = pickle.loads(msg.payload)
        p = Process(target = esperarPublicar, args= (client,t,topic,mensaje))
        p.start()
    except ValueError:
        pass
    except Exception as e:
        raise e

def main():
    client = Client()
    client.on_message = on_message
    print(f'Conectando al broker {broker}/{topic}')
    client.connect(broker)
    client.subscribe(topic)
    client.loop_forever()

if __name__ == '__main__':
    broker = "simba.fdi.ucm.es"
    if len(sys.argv) > 1:
        broker = sys.argv[1]
    topic = 'clients/mi_tema/mi_subtema'
    if len(sys.argv) > 2:
        topic = sys.argv[2]
    main()
            