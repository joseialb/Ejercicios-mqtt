"""
Ejercicio 5. Temporizador
Productor
"""


from paho.mqtt.client import Client
import pickle
import sys


def main():
    client = Client()

    print(f'Conectando al broker {broker}/{topic}')
    client.connect(broker)
    client.loop_start()
    
    m = input("parametros separados por comas: ").split(',')
    while m != "quit":
        client.publish(topic, pickle.dumps(m))
        m = input("parametros separados por comas: ").split(',')

if __name__ == '__main__':
    broker = "simba.fdi.ucm.es"
    if len(sys.argv) > 1:
        broker = sys.argv[1]
    topic = 'clients/mi_tema/mi_subtema'
    if len(sys.argv) > 2:
        topic = sys.argv[2]
    main()
            
