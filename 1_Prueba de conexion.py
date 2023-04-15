"""
Ejercicio 1. Prueba de conexion al broker
"""

from paho.mqtt.client import Client

import sys

def on_connect(client, userdata, flags, rc):
    print(f"Se ha conseguido conectar a {broker}")
    print("Connection returned result: "+ str(rc))

def on_message(client, userdata, msg):
    print("Se ha podido recibir mensajes correctamente")
    print(msg.topic, msg.payload)
    
def on_publish(client, userdata, mid):
    print("Se ha podido publicar el mensaje correctamente")

def main(broker):
    client = Client()
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_message = on_message

    print(f'Conectando al canal prueba del broker {broker}')
    client.connect(broker)

    client.subscribe('clients/mi_tema/mi_subtema')
    
    client.loop_start()
    
    a = ""
    while a != "quit":
        a = input("Mensaje?")
        client.publish('clients/mi_tema/mi_subtema', a)


if __name__ == '__main__':
    broker = "simba.fdi.ucm.es"
    main(broker)